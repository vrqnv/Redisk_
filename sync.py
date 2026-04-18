import os
import threading
import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from datetime import datetime

class SyncHandler(FileSystemEventHandler):
    def __init__(self, cloud, local_dir, remote_dir, suppress_upload):
        self.cloud = cloud
        self.local_dir = os.path.abspath(local_dir)
        self.remote_dir = remote_dir.rstrip("/") or "/"
        self.pending = {}  # для дедупликации событий
        self.suppress_upload = suppress_upload

    def _to_remote_path(self, local_path):
        rel_path = os.path.relpath(local_path, self.local_dir)
        return f"{self.remote_dir}/{rel_path}".replace("\\", "/")
    
    def on_created(self, event):
        if not event.is_directory:
            if self.suppress_upload(event.src_path):
                return
            time.sleep(0.1)  # ждём завершения записи
            rel_path = os.path.relpath(event.src_path, self.local_dir)
            remote_path = self._to_remote_path(event.src_path)
            try:
                self.cloud.upload_file(event.src_path, remote_path)
                print(f"Синхронизирован: {rel_path} -> {remote_path}")
            except Exception as e:
                print(f"Ошибка синхронизации {rel_path}: {e}")
    
    def on_deleted(self, event):
        if not event.is_directory:
            if self.suppress_upload(event.src_path):
                return
            rel_path = os.path.relpath(event.src_path, self.local_dir)
            remote_path = self._to_remote_path(event.src_path)
            try:
                self.cloud.delete(remote_path)
                print(f"Удалён: {remote_path}")
            except Exception as e:
                print(f"Ошибка удаления {rel_path}: {e}")
    
    def on_modified(self, event):
        if not event.is_directory:
            if self.suppress_upload(event.src_path):
                return
            # Избегаем двойной синхронизации
            if event.src_path in self.pending:
                return
            self.pending[event.src_path] = time.time()
            time.sleep(0.2)
            rel_path = os.path.relpath(event.src_path, self.local_dir)
            remote_path = self._to_remote_path(event.src_path)
            try:
                self.cloud.upload_file(event.src_path, remote_path)
                print(f"Обновлён: {rel_path}")
            except Exception as e:
                print(f"Ошибка обновления {rel_path}: {e}")
            finally:
                del self.pending[event.src_path]

class BidirectionalSync:
    def __init__(self, cloud, local_dir, remote_dir, poll_interval=20):
        self.cloud = cloud
        self.local_dir = os.path.abspath(local_dir)
        self.remote_dir = remote_dir.rstrip("/") or "/"
        self.poll_interval = poll_interval
        self.observer = None
        self.running = False
        self.poll_thread = None
        self.remote_snapshot = {}
        self.local_suppress_until = {}
        self.local_suppress_seconds = 3
        self.lock = threading.Lock()

    def _suppress_upload(self, local_path):
        until = self.local_suppress_until.get(local_path)
        if not until:
            return False
        if time.time() > until:
            del self.local_suppress_until[local_path]
            return False
        return True

    def _mark_local_suppressed(self, local_path):
        self.local_suppress_until[local_path] = time.time() + self.local_suppress_seconds

    def _to_remote_path(self, local_path):
        rel_path = os.path.relpath(local_path, self.local_dir)
        return f"{self.remote_dir}/{rel_path}".replace("\\", "/")

    def _to_local_path(self, remote_path):
        rel_path = remote_path[len(self.remote_dir):].lstrip("/")
        rel_path = rel_path.replace("/", os.sep)
        return os.path.join(self.local_dir, rel_path)

    def _parse_remote_modified(self, modified_str):
        if not modified_str:
            return 0
        # Yandex usually returns RFC3339 datetime.
        try:
            return datetime.fromisoformat(modified_str.replace("Z", "+00:00")).timestamp()
        except ValueError:
            return 0

    def _list_remote_files_recursive(self, path):
        result = {}
        items = self.cloud.list_files(path)
        for item in items:
            item_type = item.get("type")
            item_path = item.get("path", "")
            if not item_path:
                continue
            if item_type == "dir":
                remote_path = self.cloud.path_from_api(item_path)
                result.update(self._list_remote_files_recursive(remote_path))
                continue
            if item_type != "file":
                continue
            remote_path = self.cloud.path_from_api(item_path)
            result[remote_path] = {
                "size": item.get("size", 0),
                "modified": self._parse_remote_modified(item.get("modified")),
            }
        return result

    def _scan_local_files(self):
        local_files = {}
        for root, _, files in os.walk(self.local_dir):
            for filename in files:
                full_path = os.path.join(root, filename)
                try:
                    stat = os.stat(full_path)
                except OSError:
                    continue
                remote_path = self._to_remote_path(full_path)
                local_files[remote_path] = {
                    "local_path": full_path,
                    "size": stat.st_size,
                    "modified": stat.st_mtime,
                }
        return local_files

    def _download_remote_file(self, remote_path):
        local_path = self._to_local_path(remote_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        self._mark_local_suppressed(local_path)
        self.cloud.download_file(remote_path, local_path)

    def _upload_local_file(self, local_path):
        remote_path = self._to_remote_path(local_path)
        self.cloud.upload_file(local_path, remote_path)

    def _safe_remove_local_file(self, remote_path):
        local_path = self._to_local_path(remote_path)
        if os.path.exists(local_path):
            self._mark_local_suppressed(local_path)
            try:
                os.remove(local_path)
            except OSError:
                pass

    def _initial_reconcile(self):
        remote_files = self._list_remote_files_recursive(self.remote_dir)
        local_files = self._scan_local_files()

        remote_set = set(remote_files.keys())
        local_set = set(local_files.keys())

        only_remote = remote_set - local_set
        only_local = local_set - remote_set
        both = remote_set & local_set

        for remote_path in sorted(only_remote):
            try:
                self._download_remote_file(remote_path)
            except Exception as exc:
                print(f"Ошибка initial download {remote_path}: {exc}")

        for remote_path in sorted(only_local):
            try:
                self._upload_local_file(local_files[remote_path]["local_path"])
            except Exception as exc:
                print(f"Ошибка initial upload {remote_path}: {exc}")

        for remote_path in sorted(both):
            remote_meta = remote_files[remote_path]
            local_meta = local_files[remote_path]
            # Simple conflict policy: newer side wins.
            if remote_meta["modified"] > local_meta["modified"] + 2:
                try:
                    self._download_remote_file(remote_path)
                except Exception as exc:
                    print(f"Ошибка resolve remote->local {remote_path}: {exc}")
            elif local_meta["modified"] > remote_meta["modified"] + 2:
                try:
                    self._upload_local_file(local_meta["local_path"])
                except Exception as exc:
                    print(f"Ошибка resolve local->remote {remote_path}: {exc}")

        self.remote_snapshot = self._list_remote_files_recursive(self.remote_dir)

    def _poll_remote_loop(self):
        while self.running:
            try:
                current = self._list_remote_files_recursive(self.remote_dir)
                with self.lock:
                    previous = self.remote_snapshot
                    prev_keys = set(previous.keys())
                    curr_keys = set(current.keys())

                    added = curr_keys - prev_keys
                    removed = prev_keys - curr_keys
                    maybe_changed = curr_keys & prev_keys

                    for remote_path in sorted(added):
                        try:
                            self._download_remote_file(remote_path)
                            print(f"Cloud->Local добавлен: {remote_path}")
                        except Exception as exc:
                            print(f"Ошибка download {remote_path}: {exc}")

                    for remote_path in sorted(removed):
                        self._safe_remove_local_file(remote_path)
                        print(f"Cloud->Local удален: {remote_path}")

                    for remote_path in sorted(maybe_changed):
                        prev_meta = previous[remote_path]
                        curr_meta = current[remote_path]
                        if (
                            curr_meta["modified"] > prev_meta["modified"] + 1
                            or curr_meta["size"] != prev_meta["size"]
                        ):
                            try:
                                self._download_remote_file(remote_path)
                                print(f"Cloud->Local обновлен: {remote_path}")
                            except Exception as exc:
                                print(f"Ошибка update download {remote_path}: {exc}")

                    self.remote_snapshot = current
            except Exception as exc:
                print(f"Ошибка polling облака: {exc}")

            time.sleep(self.poll_interval)

    def start(self):
        os.makedirs(self.local_dir, exist_ok=True)
        self._initial_reconcile()

        handler = SyncHandler(
            self.cloud,
            self.local_dir,
            self.remote_dir,
            self._suppress_upload,
        )
        self.observer = Observer()
        self.observer.schedule(handler, self.local_dir, recursive=True)
        self.observer.start()

        self.running = True
        self.poll_thread = threading.Thread(target=self._poll_remote_loop, daemon=True)
        self.poll_thread.start()
        return self

    def stop(self):
        self.running = False
        if self.observer:
            self.observer.stop()
            self.observer.join(timeout=3)
            self.observer = None
        if self.poll_thread and self.poll_thread.is_alive():
            self.poll_thread.join(timeout=3)
        self.poll_thread = None


def start_sync(cloud, local_dir, remote_dir, poll_interval=20):
    """Запустить двустороннюю синхронизацию."""
    sync_worker = BidirectionalSync(cloud, local_dir, remote_dir, poll_interval=poll_interval)
    return sync_worker.start()