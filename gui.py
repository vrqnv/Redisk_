import json
import os
import re
import shutil
import sys
import tempfile

import requests
from PyQt6.QtCore import Qt, QThread, QTimer, pyqtSignal, QMimeData, QUrl
from PyQt6.QtGui import QAction, QDrag, QIcon, QPixmap
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QInputDialog,
    QLabel,
    QLineEdit,
    QListWidget,
    QMainWindow,
    QMessageBox,
    QProgressBar,
    QStatusBar,
    QSystemTrayIcon,
    QVBoxLayout,
    QWidget,
    QDialog,
    QAbstractItemView,
    QListView,
    QMenu,
)

from yandex import YandexDisk


def _safe_local_filename(name: str) -> str:
    name = os.path.basename(name.replace("\\", "/"))
    for c in '<>:"/\\|?*':
        name = name.replace(c, "_")
    return name or "file"


class DownloadThread(QThread):
    progress = pyqtSignal(int, int)
    finished = pyqtSignal(bool, str)

    def __init__(self, cloud, remote_path, local_path):
        super().__init__()
        self.cloud = cloud
        self.remote_path = remote_path
        self.local_path = local_path

    def run(self):
        try:
            def progress_callback(downloaded, total):
                self.progress.emit(downloaded, total)

            self.cloud.download_file(self.remote_path, self.local_path, progress_callback)
            self.finished.emit(True, "Скачивание завершено")
        except Exception as e:
            self.finished.emit(False, str(e))


class CloudFileListWidget(QListWidget):
    def __init__(self, owner):
        super().__init__()
        self.owner = owner
        self._dd_start_pos = None
        self._dd_item = None
        self.setAcceptDrops(True)
        self.setDragEnabled(False)
        self.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.setDefaultDropAction(Qt.DropAction.CopyAction)
        self.setMovement(QListView.Movement.Static)
        self.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)

    def mousePressEvent(self, event):
        super().mousePressEvent(event)
        if event.button() == Qt.MouseButton.LeftButton:
            self._dd_start_pos = event.pos()
            self._dd_item = self.itemAt(event.pos())

    def mouseMoveEvent(self, event):
        if (
            event.buttons() & Qt.MouseButton.LeftButton
            and self._dd_start_pos is not None
            and (event.pos() - self._dd_start_pos).manhattanLength()
            >= QApplication.startDragDistance()
        ):
            item = self._dd_item or self.itemAt(event.pos())
            self._dd_start_pos = None
            self._dd_item = None
            if item:
                name, is_dir = self.owner.parse_list_item(item.text())
                if name and not is_dir:
                    self._export_file_drag(name)
                    return
        super().mouseMoveEvent(event)

    def mouseReleaseEvent(self, event):
        self._dd_start_pos = None
        self._dd_item = None
        super().mouseReleaseEvent(event)

    def _export_file_drag(self, name: str):
        remote_path = self.owner.remote_path_for_name(name)
        tmpdir = tempfile.mkdtemp(prefix="discohack_drag_")
        local_name = _safe_local_filename(name)
        local_path = os.path.join(tmpdir, local_name)
        QApplication.setOverrideCursor(Qt.CursorShape.WaitCursor)
        try:
            self.owner.cloud.download_file(remote_path, local_path)
        except Exception as e:
            QApplication.restoreOverrideCursor()
            shutil.rmtree(tmpdir, ignore_errors=True)
            QMessageBox.critical(self.owner, "Ошибка", str(e))
            return
        finally:
            QApplication.restoreOverrideCursor()

        mime = QMimeData()
        mime.setUrls([QUrl.fromLocalFile(os.path.normpath(local_path))])
        drag = QDrag(self)
        drag.setMimeData(mime)
        drag.exec(Qt.DropAction.CopyAction)

        def cleanup():
            shutil.rmtree(tmpdir, ignore_errors=True)

        QTimer.singleShot(120_000, cleanup)

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dragMoveEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()
        else:
            event.ignore()

    def dropEvent(self, event):
        if not event.mimeData().hasUrls():
            event.ignore()
            return

        paths = []
        for url in event.mimeData().urls():
            if url.isLocalFile():
                paths.append(url.toLocalFile())

        if not paths:
            event.ignore()
            return

        action = event.dropAction()
        if action == Qt.DropAction.IgnoreAction:
            action = event.proposedDropAction()
        move_sources = action == Qt.DropAction.MoveAction

        self.owner.upload_paths(paths, ask_confirmation=False, delete_sources_after=move_sources)
        event.setDropAction(Qt.DropAction.MoveAction if move_sources else Qt.DropAction.CopyAction)
        event.accept()


class CloudExplorer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DiscoHack - Яндекс.Диск")
        self.setGeometry(100, 100, 900, 600)

        self.cfg_path = "config.json"
        self.cfg = self.load_config()
        self.ensure_token()

        self.cloud = YandexDisk(self.cfg["yandex_token"])
        self.current_path = "/"
        self.current_items = []

        self.setup_ui()
        self.setup_tray()
        self.setup_statusbar()
        self.load_cloud_files()

    def load_config(self):
        if os.path.exists(self.cfg_path):
            with open(self.cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {"service": "yandex", "yandex_token": "", "tray_notifications": True}

    def notifications_enabled(self) -> bool:
        return bool(self.cfg.get("tray_notifications", True))

    def tray_show_message(
        self,
        title: str,
        message: str,
        icon=QSystemTrayIcon.MessageIcon.Information,
        ms: int = 3000,
    ):
        if not self.notifications_enabled():
            return
        self.tray_icon.showMessage(title, message, icon, ms)

    def save_config(self):
        with open(self.cfg_path, "w", encoding="utf-8") as f:
            json.dump(self.cfg, f, ensure_ascii=False, indent=4)

    def ensure_token(self):
        token = self.cfg.get("yandex_token", "").strip()
        if token and token != "ВАШ_ТОКЕН_ЯНДЕКСА":
            return
        token, ok = QInputDialog.getText(
            self,
            "Авторизация Яндекс.Диск",
            "Введите OAuth токен Яндекс.Диска:",
            QLineEdit.EchoMode.Normal,
        )
        if not ok or not token.strip():
            QMessageBox.critical(self, "Ошибка", "Токен обязателен для работы приложения.")
            sys.exit(1)
        self.cfg["yandex_token"] = token.strip()
        self.save_config()

    def setup_ui(self):
        self.setCentralWidget(self.create_cloud_tab())

    def create_cloud_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        self.search_input = QLineEdit()
        self.search_input.setPlaceholderText("Поиск по текущему каталогу...")
        self.search_input.textChanged.connect(self.apply_search_filter)

        self.file_list = CloudFileListWidget(self)
        self.file_list.itemDoubleClicked.connect(self.on_item_double_click)

        self.file_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.file_list.customContextMenuRequested.connect(self.show_file_context_menu)

        layout.addWidget(self.search_input)
        layout.addWidget(self.file_list)
        widget.setLayout(layout)
        return widget

    def setup_statusbar(self):
        self.statusbar = QStatusBar()
        self.setStatusBar(self.statusbar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setVisible(False)
        self.statusbar.addPermanentWidget(self.progress_bar)

    def setup_tray(self):
        self.tray_icon = QSystemTrayIcon(self)
        tray_icon = QIcon()
        icon_path = os.path.join(os.path.dirname(__file__), "assets", "tray.png")
        if os.path.exists(icon_path):
            base = QPixmap(icon_path)
            if not base.isNull():
                for size in (16, 20, 24, 32, 48, 64):
                    tray_icon.addPixmap(
                        base.scaled(
                            size,
                            size,
                            Qt.AspectRatioMode.KeepAspectRatio,
                            Qt.TransformationMode.SmoothTransformation,
                        )
                    )
        if tray_icon.isNull():
            tray_icon = QIcon.fromTheme("folder-remote")
        self.tray_icon.setIcon(tray_icon)
        self.setWindowIcon(tray_icon)

        tray_menu = QMenu()
        show_action = QAction("Показать окно", self)
        show_action.triggered.connect(self.showNormal)

        hide_action = QAction("Скрыть окно", self)
        hide_action.triggered.connect(self.hide)

        self._tray_notify_action = QAction("Уведомления", self)
        self._tray_notify_action.setCheckable(True)
        self._tray_notify_action.blockSignals(True)
        self._tray_notify_action.setChecked(self.notifications_enabled())
        self._tray_notify_action.blockSignals(False)
        self._tray_notify_action.toggled.connect(self._on_tray_notifications_toggled)

        quit_action = QAction("Выйти", self)
        quit_action.triggered.connect(self.quit_app)

        tray_menu.addAction(show_action)
        tray_menu.addAction(hide_action)
        tray_menu.addSeparator()
        tray_menu.addAction(self._tray_notify_action)
        tray_menu.addSeparator()
        tray_menu.addAction(quit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(self.on_tray_activated)
        self.tray_icon.show()
        self.tray_show_message(
            "DiscoHack",
            "Приложение запущено в tray. Нажмите по иконке для открытия.",
            QSystemTrayIcon.MessageIcon.Information,
            3000,
        )

    def _on_tray_notifications_toggled(self, checked: bool):
        self.cfg["tray_notifications"] = checked
        self.save_config()

    def on_tray_activated(self, reason):
        if reason in (
            QSystemTrayIcon.ActivationReason.Trigger,
            QSystemTrayIcon.ActivationReason.DoubleClick,
        ):
            if self.isVisible():
                self.hide()
            else:
                self.showNormal()
                self.activateWindow()

    def closeEvent(self, event):
        event.ignore()
        self.hide()
        self.tray_show_message(
            "DiscoHack",
            "Окно скрыто в tray. Для выхода используйте меню иконки.",
            QSystemTrayIcon.MessageIcon.Information,
            2500,
        )

    def show_file_context_menu(self, pos):
        menu = QMenu(self)
        refresh = QAction("Обновить", self)
        refresh.setToolTip(
            "Запрашивает у сервера Яндекс.Диска актуальный список файлов и папок "
            "в текущей директории и обновляет отображение в окне."
        )
        refresh.triggered.connect(self.load_cloud_files)
        menu.addAction(refresh)
        for title, callback in (
            ("Наверх", self.go_up),
            ("Создать файл", self.create_empty_remote_file),
            ("Скачать", self.download_selected),
            ("Удалить", self.delete_selected),
            ("Предпросмотр", self.preview_selected),
        ):
            act = QAction(title, self)
            act.triggered.connect(callback)
            menu.addAction(act)
        menu.exec(self.file_list.mapToGlobal(pos))

    def load_cloud_files(self):
        try:
            self.statusbar.showMessage(f"Загрузка {self.current_path}...")
            items = self.cloud.list_files(self.current_path)
            self.current_items = []

            folders = [item for item in items if item.get("type") == "dir"]
            files = [item for item in items if item.get("type") == "file"]

            for item in folders + files:
                name = item.get("name", "unknown")
                item_type = item.get("type", "file")
                size = item.get("size", 0)

                if item_type == "dir":
                    display_text = f"📁 {name}/"
                else:
                    if size < 1024:
                        size_str = f"{size} B"
                    elif size < 1024 * 1024:
                        size_str = f"{size / 1024:.1f} KB"
                    else:
                        size_str = f"{size / (1024 * 1024):.1f} MB"
                    display_text = f"📄 {name} ({size_str})"

                self.current_items.append(display_text)

            self.apply_search_filter()
            self.statusbar.showMessage(f"Загружено {len(items)} элементов", 3000)

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить файлы: {e}")
            self.statusbar.showMessage(f"Ошибка: {e}", 5000)

    def parse_list_item(self, text):
        if text.startswith("📁 "):
            return text[2:-1], True
        if text.startswith("📄 "):
            name_with_size = text[2:]
            last_paren = name_with_size.rfind("(")
            if last_paren > 0:
                return name_with_size[:last_paren].strip(), False
            return name_with_size, False
        return None, False

    def on_item_double_click(self, item):
        text = item.text()
        if text.startswith("📁 "):
            name = text[2:-1]
            self.current_path = self.current_path.rstrip("/") + "/" + name
            self.load_cloud_files()
        elif text.startswith("📄 "):
            self.download_selected()

    def go_up(self):
        if self.current_path != "/":
            parent = os.path.dirname(self.current_path.rstrip("/"))
            self.current_path = parent if parent else "/"
            self.load_cloud_files()

    def apply_search_filter(self):
        query = self.search_input.text().strip().lower()
        self.file_list.clear()
        if not query:
            for item in self.current_items:
                self.file_list.addItem(item)
            return
        for item in self.current_items:
            name, _ = self.parse_list_item(item)
            if name and query in name.lower():
                self.file_list.addItem(item)

    def get_selected_name(self):
        current = self.file_list.currentItem()
        if not current:
            return None
        name, _ = self.parse_list_item(current.text())
        return name

    def remote_path_for_name(self, name):
        return self.current_path.rstrip("/") + "/" + name

    def download_selected(self, local_path=None):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите файл")
            return

        remote_path = self.remote_path_for_name(name)
        if local_path is None:
            local_path, _ = QFileDialog.getSaveFileName(self, "Сохранить как", name)

        if local_path:
            self.thread = DownloadThread(self.cloud, remote_path, local_path)
            self.thread.progress.connect(self.update_progress)
            self.thread.finished.connect(self.download_finished)
            self.thread.start()
            self.progress_bar.setVisible(True)

    def upload_paths(self, paths, ask_confirmation=False, delete_sources_after=False):
        if not paths:
            return
        all_files = []
        for path in paths:
            if os.path.isfile(path):
                all_files.append(path)
            elif os.path.isdir(path):
                for root, _, files in os.walk(path):
                    for filename in files:
                        all_files.append(os.path.join(root, filename))

        if not all_files:
            return

        if ask_confirmation:
            reply = QMessageBox.question(
                self,
                "Подтверждение",
                f"Загрузить {len(all_files)} файл(ов)\nв {self.current_path}?",
                QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
            )
            if reply != QMessageBox.StandardButton.Yes:
                return

        uploaded = []
        for file_path in all_files:
            try:
                filename = os.path.basename(file_path)
                remote_path = self.current_path.rstrip("/") + "/" + filename
                self.cloud.upload_file(file_path, remote_path)
                uploaded.append(file_path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить {file_path}: {e}")
                break

        if delete_sources_after and uploaded:
            for p in uploaded:
                try:
                    os.remove(p)
                except OSError:
                    pass

        self.load_cloud_files()

    def create_empty_remote_file(self):
        name, ok = QInputDialog.getText(self, "Новый файл", "Имя файла на диске:")
        if not ok:
            return
        name = name.strip().replace("\\", "/")
        if not name:
            return
        name = os.path.basename(name)
        if not re.match(r"^[^<>:\"/\\|?*]+\Z", name):
            QMessageBox.warning(self, "Внимание", "Недопустимое имя файла.")
            return
        remote_path = self.current_path.rstrip("/") + "/" + name
        try:
            self.cloud.upload_bytes(b"", remote_path)
            self.load_cloud_files()
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def delete_selected(self):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите элемент")
            return

        remote_path = self.remote_path_for_name(name)
        reply = QMessageBox.question(
            self,
            "Подтверждение",
            f"Удалить {name}?\nЭто действие нельзя отменить.",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No,
        )
        if reply == QMessageBox.StandardButton.Yes:
            try:
                self.cloud.delete(remote_path)
                self.load_cloud_files()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def preview_selected(self):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите файл")
            return

        ext = os.path.splitext(name)[1].lower()
        if ext not in [".jpg", ".jpeg", ".png", ".gif", ".bmp"]:
            QMessageBox.warning(self, "Предупреждение", "Это не изображение")
            return

        remote_path = self.remote_path_for_name(name)
        try:
            preview_url = self.cloud.get_preview(remote_path, "300x300")
            resp = requests.get(preview_url)
            if resp.status_code == 200:
                preview_dialog = QDialog(self)
                preview_dialog.setWindowTitle(f"Предпросмотр: {name}")
                layout = QVBoxLayout()
                pixmap = QPixmap()
                pixmap.loadFromData(resp.content)
                label = QLabel()
                label.setPixmap(pixmap.scaled(400, 400, Qt.AspectRatioMode.KeepAspectRatio))
                layout.addWidget(label)
                preview_dialog.setLayout(layout)
                preview_dialog.exec()
            else:
                QMessageBox.warning(self, "Ошибка", "Не удалось загрузить превью")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def update_progress(self, downloaded, total):
        if total > 0:
            percent = int(downloaded / total * 100)
            self.progress_bar.setValue(percent)
            self.statusbar.showMessage(f"Скачивание: {percent}%")

    def download_finished(self, success, message):
        self.progress_bar.setVisible(False)
        if success:
            self.statusbar.showMessage(message, 3000)
        else:
            QMessageBox.critical(self, "Ошибка", message)

    def quit_app(self):
        self.tray_icon.hide()
        QApplication.quit()


def main():
    app = QApplication(sys.argv)
    app.setQuitOnLastWindowClosed(False)

    window = CloudExplorer()
    window.hide()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
