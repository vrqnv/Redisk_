import shutil
import subprocess


YANDEX_WEBDAV_URL = "davs://webdav.yandex.ru"


def _run_command(command):
    """Run a command and return (success, output)."""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            check=False,
        )
        output = (result.stdout or "") + (result.stderr or "")
        return result.returncode == 0, output.strip()
    except Exception as exc:
        return False, str(exc)


def mount_cloud(service_name):
    """Mount cloud storage via gio/gvfs."""
    if service_name != "yandex":
        return False, f"Неподдерживаемый сервис: {service_name}"

    if shutil.which("gio"):
        ok, output = _run_command(["gio", "mount", YANDEX_WEBDAV_URL])
        if ok:
            return True, "Монтирование выполнено через gio"
        # If already mounted, gio usually returns non-zero with this phrase.
        if "already mounted" in output.lower():
            return True, "Ресурс уже смонтирован"

    if shutil.which("gvfs-mount"):
        ok, output = _run_command(["gvfs-mount", YANDEX_WEBDAV_URL])
        if ok:
            return True, "Монтирование выполнено через gvfs-mount"
        return False, f"Не удалось смонтировать: {output}"

    return False, "Не найдено gio/gvfs-mount. Установите gvfs."


def unmount_cloud(service_name):
    """Unmount cloud storage via gio/gvfs."""
    if service_name != "yandex":
        return False, f"Неподдерживаемый сервис: {service_name}"

    if shutil.which("gio"):
        ok, output = _run_command(["gio", "mount", "-u", YANDEX_WEBDAV_URL])
        if ok:
            return True, "Размонтирование выполнено через gio"
        if "not mounted" in output.lower():
            return True, "Ресурс уже размонтирован"

    if shutil.which("gvfs-mount"):
        ok, output = _run_command(["gvfs-mount", "-u", YANDEX_WEBDAV_URL])
        if ok:
            return True, "Размонтирование выполнено через gvfs-mount"
        return False, f"Не удалось размонтировать: {output}"

    return False, "Не найдено gio/gvfs-mount. Установите gvfs."


def is_cloud_mounted(service_name):
    """Check whether cloud storage is currently mounted."""
    if service_name != "yandex":
        return False

    if shutil.which("gio"):
        ok, output = _run_command(["gio", "mount", "-l"])
        if ok and "webdav.yandex.ru" in output.lower():
            return True

    if shutil.which("gvfs-mount"):
        ok, output = _run_command(["gvfs-mount", "-l"])
        if ok and "webdav.yandex.ru" in output.lower():
            return True

    return False