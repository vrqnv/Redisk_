import sys
import os
import json
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QVBoxLayout, QHBoxLayout,
    QWidget, QPushButton, QFileDialog, QMessageBox, QSystemTrayIcon,
    QMenu, QLabel, QDialog, QLineEdit, QInputDialog, QProgressBar,
    QStatusBar, QTextEdit, QTabWidget, QListWidget, QAbstractItemView
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction, QIcon, QPixmap

from yandex import YandexDisk
from cache import FileCache
from sync import start_sync
from integration import mount_cloud, unmount_cloud, is_cloud_mounted

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
        self.setAcceptDrops(True)
        self.setDragDropMode(QAbstractItemView.DragDropMode.DropOnly)
        self.setDefaultDropAction(Qt.DropAction.CopyAction)

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

        if paths:
            self.owner.upload_paths(paths)
            event.acceptProposedAction()
        else:
            event.ignore()


class CloudExplorer(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("DiscoHack - Яндекс.Диск")
        self.setGeometry(100, 100, 900, 600)

        self.cfg_path = "config.json"
        self.cfg = self.load_config()
        self.ensure_token()

        self.cloud = YandexDisk(self.cfg["yandex_token"])
        self.cache = FileCache(self.cfg.get("cache_dir", "/var/tmp/discohack_cache"))
        self.sync_worker = None
        self.current_path = "/"

        self.setup_ui()
        self.setup_tray()
        self.setup_statusbar()
        self.update_mount_status()
        self.load_cloud_files()

    def load_config(self):
        if os.path.exists(self.cfg_path):
            with open(self.cfg_path, "r", encoding="utf-8") as f:
                return json.load(f)
        return {
            "service": "yandex",
            "yandex_token": "",
            "cache_dir": "/var/tmp/discohack_cache",
            "sync_enabled": False
        }

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
        tabs = QTabWidget()
        cloud_widget = self.create_cloud_tab()
        tabs.addTab(cloud_widget, "Облако")

        sync_widget = self.create_sync_tab()
        tabs.addTab(sync_widget, "Синхронизация")

        links_widget = self.create_links_tab()
        tabs.addTab(links_widget, "Публичные ссылки")

        self.setCentralWidget(tabs)

    def create_cloud_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        path_layout = QHBoxLayout()
        self.path_label = QLabel("Текущий путь: /")
        self.path_input = QLineEdit()
        self.path_input.setPlaceholderText("Введите путь и нажмите Enter")
        self.path_input.returnPressed.connect(self.go_to_path)
        path_layout.addWidget(self.path_label)
        path_layout.addWidget(self.path_input)

        self.file_list = CloudFileListWidget(self)
        self.file_list.itemDoubleClicked.connect(self.on_item_double_click)

        self.file_list.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.file_list.customContextMenuRequested.connect(self.show_file_context_menu)

        hint = QLabel("Подсказка: перетащите файлы/папки в список для загрузки в текущую директорию облака.")

        layout.addLayout(path_layout)
        layout.addWidget(self.file_list)
        layout.addWidget(hint)
        widget.setLayout(layout)
        return widget

    def create_sync_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        self.sync_status = QLabel("Синхронизация не запущена")
        self.sync_local_path = QLineEdit()
        self.sync_local_path.setPlaceholderText("Путь к локальной папке (например /home/user/sync)")

        self.sync_remote_path = QLineEdit()
        self.sync_remote_path.setPlaceholderText("Путь в облаке (например /sync)")

        btn_start_sync = QPushButton("Запустить синхронизацию")
        btn_start_sync.clicked.connect(self.start_sync_folder)

        btn_stop_sync = QPushButton("Остановить синхронизацию")
        btn_stop_sync.clicked.connect(self.stop_sync)

        self.mount_status = QLabel("Статус монтирования: неизвестно")
        btn_mount = QPushButton("Смонтировать диск (GVfs)")
        btn_mount.clicked.connect(self.mount_disk)
        btn_unmount = QPushButton("Размонтировать диск")
        btn_unmount.clicked.connect(self.unmount_disk)

        layout.addWidget(QLabel("Локальная папка:"))
        layout.addWidget(self.sync_local_path)
        layout.addWidget(QLabel("Облачная папка:"))
        layout.addWidget(self.sync_remote_path)
        layout.addWidget(btn_start_sync)
        layout.addWidget(btn_stop_sync)
        layout.addWidget(self.sync_status)
        layout.addWidget(self.mount_status)
        layout.addWidget(btn_mount)
        layout.addWidget(btn_unmount)
        layout.addStretch()

        widget.setLayout(layout)
        return widget

    def create_links_tab(self):
        widget = QWidget()
        layout = QVBoxLayout()

        self.links_text = QTextEdit()
        self.links_text.setReadOnly(True)
        self.links_text.setPlaceholderText("Здесь будут созданные публичные ссылки")

        btn_refresh_links = QPushButton("Обновить")
        btn_refresh_links.clicked.connect(self.load_links)

        layout.addWidget(btn_refresh_links)
        layout.addWidget(self.links_text)
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
        self.tray_icon.setIcon(QIcon.fromTheme("folder-remote"))

        tray_menu = QMenu()
        show_action = QAction("Показать окно", self)
        show_action.triggered.connect(self.showNormal)

        hide_action = QAction("Скрыть окно", self)
        hide_action.triggered.connect(self.hide)

        auth_action = QAction("Авторизация (токен)", self)
        auth_action.triggered.connect(self.configure_token)

        mount_action = QAction("Добавить диск (mount)", self)
        mount_action.triggered.connect(self.mount_disk)

        unmount_action = QAction("Удалить диск (unmount)", self)
        unmount_action.triggered.connect(self.unmount_disk)

        sync_action = QAction("Запустить синхронизацию", self)
        sync_action.triggered.connect(lambda: self.start_sync_folder())

        quit_action = QAction("Выйти", self)
        quit_action.triggered.connect(self.quit_app)

        tray_menu.addAction(show_action)
        tray_menu.addAction(hide_action)
        tray_menu.addSeparator()
        tray_menu.addAction(auth_action)
        tray_menu.addAction(mount_action)
        tray_menu.addAction(unmount_action)
        tray_menu.addSeparator()
        tray_menu.addAction(sync_action)
        tray_menu.addSeparator()
        tray_menu.addAction(quit_action)

        self.tray_icon.setContextMenu(tray_menu)
        self.tray_icon.activated.connect(self.on_tray_activated)
        self.tray_icon.show()
        self.tray_icon.showMessage(
            "DiscoHack",
            "Приложение запущено в tray. Нажмите по иконке для открытия.",
            QSystemTrayIcon.MessageIcon.Information,
            3000,
        )

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
        self.tray_icon.showMessage(
            "DiscoHack",
            "Окно скрыто в tray. Для выхода используйте меню иконки.",
            QSystemTrayIcon.MessageIcon.Information,
            2500,
        )

    def show_file_context_menu(self, pos):
        menu = QMenu(self)
        actions = [
            ("Обновить", self.load_cloud_files),
            ("Наверх", self.go_up),
            ("Загрузить файл", self.upload_file),
            ("Скачать", self.download_selected),
            ("Удалить", self.delete_selected),
            ("Предпросмотр", self.preview_selected),
            ("Создать ссылку", self.share_selected),
            ("Переместить", self.move_selected),
        ]
        for title, callback in actions:
            action = QAction(title, self)
            action.triggered.connect(callback)
            menu.addAction(action)
        menu.exec(self.file_list.mapToGlobal(pos))

    def load_cloud_files(self):
        try:
            self.statusbar.showMessage(f"Загрузка {self.current_path}...")
            items = self.cloud.list_files(self.current_path)
            self.file_list.clear()

            folders = [item for item in items if item.get('type') == 'dir']
            files = [item for item in items if item.get('type') == 'file']

            for item in folders + files:
                name = item.get('name', 'unknown')
                item_type = item.get('type', 'file')
                size = item.get('size', 0)

                if item_type == 'dir':
                    display_text = f"📁 {name}/"
                else:
                    if size < 1024:
                        size_str = f"{size} B"
                    elif size < 1024*1024:
                        size_str = f"{size/1024:.1f} KB"
                    else:
                        size_str = f"{size/(1024*1024):.1f} MB"
                    display_text = f"📄 {name} ({size_str})"

                self.file_list.addItem(display_text)

            self.path_label.setText(f"Текущий путь: {self.current_path}")
            self.statusbar.showMessage(f"Загружено {len(items)} элементов", 3000)

        except Exception as e:
            QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить файлы: {e}")
            self.statusbar.showMessage(f"Ошибка: {e}", 5000)

    def on_item_double_click(self, item):
        text = item.text()
        if text.startswith("📁 "):
            name = text[2:-1]
            self.current_path = self.current_path.rstrip('/') + '/' + name
            self.load_cloud_files()
        elif text.startswith("📄 "):
            self.download_selected()

    def go_up(self):
        if self.current_path != "/":
            parent = os.path.dirname(self.current_path.rstrip('/'))
            self.current_path = parent if parent else "/"
            self.load_cloud_files()

    def go_to_path(self):
        path = self.path_input.text().strip()
        if path:
            if not path.startswith('/'):
                path = '/' + path
            self.current_path = path
            self.load_cloud_files()
            self.path_input.clear()

    def get_selected_name(self):
        current = self.file_list.currentItem()
        if not current:
            return None
        text = current.text()
        if text.startswith("📁 "):
            return text[2:-1]
        elif text.startswith("📄 "):
            name_with_size = text[2:]
            last_paren = name_with_size.rfind('(')
            if last_paren > 0:
                return name_with_size[:last_paren].strip()
            return name_with_size
        return text

    def remote_path_for_name(self, name):
        return self.current_path.rstrip('/') + '/' + name

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

    def upload_file(self):
        file_path, _ = QFileDialog.getOpenFileName(self, "Выберите файл")
        if file_path:
            self.upload_paths([file_path], ask_confirmation=True)

    def upload_paths(self, paths, ask_confirmation=False):
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
            reply = QMessageBox.question(self, "Подтверждение", 
                                        f"Загрузить {len(all_files)} файл(ов)\nв {self.current_path}?",
                                        QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)

            if reply != QMessageBox.StandardButton.Yes:
                return

        for file_path in all_files:
            try:
                filename = os.path.basename(file_path)
                remote_path = self.current_path.rstrip('/') + '/' + filename
                self.cloud.upload_file(file_path, remote_path)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не удалось загрузить {file_path}: {e}")
                break

        self.load_cloud_files()

    def delete_selected(self):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите элемент")
            return

        remote_path = self.remote_path_for_name(name)
        reply = QMessageBox.question(self, "Подтверждение", 
                                    f"Удалить {name}?\nЭто действие нельзя отменить.",
                                    QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No)
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
        if ext not in ['.jpg', '.jpeg', '.png', '.gif', '.bmp']:
            QMessageBox.warning(self, "Предупреждение", "Это не изображение")
            return

        remote_path = self.remote_path_for_name(name)
        try:
            preview_url = self.cloud.get_preview(remote_path, "300x300")
            import requests
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

    def share_selected(self):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите элемент")
            return

        remote_path = self.remote_path_for_name(name)
        try:
            link = self.cloud.publish(remote_path)
            self.links_text.append(f"{name}: {link}")
            QMessageBox.information(self, "Ссылка создана", f"Публичная ссылка:\n{link}")
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def move_selected(self):
        name = self.get_selected_name()
        if not name:
            QMessageBox.warning(self, "Внимание", "Выберите элемент")
            return

        from_path = self.remote_path_for_name(name)
        to_path, ok = QInputDialog.getText(self, "Переместить", 
                                          f"Переместить {name}\nНовый путь в облаке:")
        if ok and to_path:
            if not to_path.startswith('/'):
                to_path = '/' + to_path
            try:
                self.cloud.move(from_path, to_path)
                self.load_cloud_files()
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", str(e))

    def configure_token(self):
        token, ok = QInputDialog.getText(
            self,
            "Обновить токен",
            "Введите OAuth токен Яндекс.Диска:",
            QLineEdit.EchoMode.Normal,
            self.cfg.get("yandex_token", ""),
        )
        if ok and token.strip():
            self.cfg["yandex_token"] = token.strip()
            self.save_config()
            self.cloud = YandexDisk(self.cfg["yandex_token"])
            QMessageBox.information(self, "Готово", "Токен обновлен.")

    def update_mount_status(self):
        mounted = is_cloud_mounted("yandex")
        self.mount_status.setText(
            "Статус монтирования: смонтирован" if mounted else "Статус монтирования: не смонтирован"
        )

    def mount_disk(self):
        ok, message = mount_cloud("yandex")
        if ok:
            self.statusbar.showMessage(message, 4000)
            self.tray_icon.showMessage("DiscoHack", message, QSystemTrayIcon.MessageIcon.Information, 2500)
        else:
            QMessageBox.warning(self, "Монтирование", message)
        self.update_mount_status()

    def unmount_disk(self):
        ok, message = unmount_cloud("yandex")
        if ok:
            self.statusbar.showMessage(message, 4000)
        else:
            QMessageBox.warning(self, "Размонтирование", message)
        self.update_mount_status()

    def start_sync_folder(self):
        local_dir = self.sync_local_path.text().strip()
        remote_dir = self.sync_remote_path.text().strip()

        if not local_dir or not remote_dir:
            QMessageBox.warning(self, "Ошибка", "Укажите локальную и облачную папки")
            return

        if not os.path.exists(local_dir):
            try:
                os.makedirs(local_dir)
            except Exception as e:
                QMessageBox.critical(self, "Ошибка", f"Не могу создать локальную папку: {e}")
                return

        try:
            self.stop_sync()
            self.sync_worker = start_sync(self.cloud, local_dir, remote_dir, poll_interval=20)
            self.sync_status.setText(f"Синхронизация запущена: {local_dir} ↔ {remote_dir}")
            self.statusbar.showMessage("Двусторонняя синхронизация запущена", 4000)
        except Exception as e:
            QMessageBox.critical(self, "Ошибка", str(e))

    def stop_sync(self):
        if self.sync_worker:
            self.sync_worker.stop()
            self.sync_worker = None
            self.sync_status.setText("Синхронизация остановлена")

    def load_links(self):
        self.links_text.append("--- Ссылки хранятся в истории ---")

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
        if self.sync_worker:
            self.sync_worker.stop()
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