import requests
import os
import time

class YandexDisk:
    def __init__(self, token):
        self.token = token
        self.base_url = "https://cloud-api.yandex.net/v1/disk"

    def _headers(self):
        return {"Authorization": f"OAuth {self.token}"}

    def path_from_api(self, api_path):
        """Convert API path like disk:/foo to /foo."""
        if not api_path:
            return "/"
        if api_path.startswith("disk:/"):
            return "/" + api_path[len("disk:/"):].lstrip("/")
        return api_path

    def _ensure_remote_parent_dirs(self, remote_path):
        parent_dir = os.path.dirname(remote_path.rstrip("/"))
        if not parent_dir or parent_dir == "/":
            return
        parts = parent_dir.strip("/").split("/")
        current = ""
        for part in parts:
            current += f"/{part}"
            self.create_dir(current)

    # Навигация
    def list_files(self, path="/", limit=100):
        url = f"{self.base_url}/resources?path={path}&limit={limit}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        items = resp.json().get("_embedded", {}).get("items", [])
        return items

    # Скачивание
    def download_file(self, remote_path, local_path, progress_callback=None):
        url = f"{self.base_url}/resources/download?path={remote_path}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        href = resp.json()["href"]
        
        r = requests.get(href, stream=True)
        total_size = int(r.headers.get('content-length', 0))
        downloaded = 0
        
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
                downloaded += len(chunk)
                if progress_callback and total_size:
                    progress_callback(downloaded, total_size)

    # Загрузка
    def upload_file(self, local_path, remote_path, progress_callback=None):
        self._ensure_remote_parent_dirs(remote_path)
        url = f"{self.base_url}/resources/upload?path={remote_path}&overwrite=true"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        upload_url = resp.json()["href"]
        
        with open(local_path, "rb") as f:
            r = requests.put(upload_url, data=f, stream=True)
            r.raise_for_status()

    # Удаление
    def delete(self, remote_path, permanently=False):
        url = f"{self.base_url}/resources?path={remote_path}&permanently={str(permanently).lower()}"
        resp = requests.delete(url, headers=self._headers())
        resp.raise_for_status()

    # Перемещение
    def move(self, from_path, to_path):
        url = f"{self.base_url}/resources/move"
        payload = {"from": from_path, "path": to_path, "overwrite": True}
        resp = requests.post(url, headers=self._headers(), params=payload)
        resp.raise_for_status()

    def create_dir(self, remote_path):
        url = f"{self.base_url}/resources?path={remote_path}"
        resp = requests.put(url, headers=self._headers())
        # 201: created, 409: already exists
        if resp.status_code not in (201, 409):
            resp.raise_for_status()

    # Создать публичную ссылку
    def publish(self, remote_path):
        url = f"{self.base_url}/resources/publish?path={remote_path}"
        resp = requests.put(url, headers=self._headers())
        resp.raise_for_status()
        return resp.json().get("href")

    # Удалить публичную ссылку
    def unpublish(self, remote_path):
        url = f"{self.base_url}/resources/unpublish?path={remote_path}"
        resp = requests.put(url, headers=self._headers())
        return resp.status_code == 204

    # Получить информацию о файле
    def get_meta(self, remote_path):
        url = f"{self.base_url}/resources?path={remote_path}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        return resp.json()

    # Получить превью (для предпросмотра фото)
    def get_preview(self, remote_path, size="150x150"):
        url = f"{self.base_url}/resources/download?path={remote_path}"
        resp = requests.get(url, headers=self._headers())
        resp.raise_for_status()
        download_url = resp.json()["href"]
        # Яндекс сам отдаст превью, если добавить параметр preview
        preview_url = f"{download_url}&preview=true&size={size}"
        return preview_url