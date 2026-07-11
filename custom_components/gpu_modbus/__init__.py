import os
import platform
import subprocess
import logging
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.components.frontend import async_register_panel, async_remove_panel

_LOGGER = logging.getLogger(__name__)
DOMAIN = "gpu_modbus"
go_process = None

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global go_process
    
    current_dir = os.path.dirname(__file__)
    arch = platform.machine()
    
    if "x86_64" in arch or "amd64" in arch:
        bin_name = "gpu_bridge_amd64"
    elif "arm" in arch or "aarch64" in arch:
        bin_name = "gpu_bridge_arm64"
    else:
        _LOGGER.error(f"Архитектура {arch} не поддерживается")
        return False

    bin_path = os.path.join(current_dir, "bin", bin_name)
    
    try:
        os.chmod(bin_path, 0o755)
    except Exception as e:
        _LOGGER.warning(f"Не удалось установить права +x на бинарник: {e}")

    env = os.environ.copy()
    env["CONTROLLERS_PATH"] = os.path.join(current_dir, "controllers")
    env["WEB_PATH"] = os.path.join(current_dir, "web")
    env["DATA_PATH"] = os.path.join(hass.config.config_dir, "gpu_modbus_data")
    
    _LOGGER.info(f"Запуск Go-моста ГПУ: {bin_path}")
    go_process = subprocess.Popen(
        [bin_path],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # --- ДОБАВЛЕНИЕ НА БОКОВУЮ ПАНЕЛЬ ---
    # Мы регистрируем iframe, который откроет веб-морду Go (порт 8080) прямо внутри Home Assistant
    async_register_panel(
        hass,
        frontend_url_path="gpu_modbus_panel",       # URL-путь внутри HA
        webcomponent_name="ha-panel-iframe",        # Тип панели (встроенный iframe)
        sidebar_title="Панель ГПУ",                 # Имя кнопки на боковой панели
        sidebar_icon="mdi:engine-outline",          # Иконка (промышленный мотор/двигатель)
        config={"url": "http://localhost:8080"},    # Ссылка на локальный порт Go
        require_admin=False                         # Доступно всем или только админам (True)
    )

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global go_process
    
    # --- УДАЛЕНИЕ С БОКОВОЙ ПАНЕЛИ ---
    # Если пользователь удалит интеграцию, кнопка сбоку тоже пропадет
    async_remove_panel(hass, "gpu_modbus_panel")

    if go_process:
        _LOGGER.info("Остановка Go-моста ГПУ...")
        go_process.terminate()
        go_process.wait()
        go_process = None
    return True