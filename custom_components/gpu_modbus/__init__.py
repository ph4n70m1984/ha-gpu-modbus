import os
import platform
import subprocess
import logging
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.components import frontend

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

    # Подготовка переменных окружения для Go-процесса
    env = os.environ.copy()
    env["CONTROLLERS_PATH"] = os.path.join(current_dir, "controllers")
    env["WEB_PATH"] = os.path.join(current_dir, "web")
    env["DATA_PATH"] = os.path.join(hass.config.config_dir, "gpu_modbus_data")
    
    # --- НАСТРОЙКИ MQTT ---
    mqtt_broker = entry.data.get("mqtt_broker", "127.0.0.1:1883")
    if not mqtt_broker.startswith("tcp://") and not mqtt_broker.startswith("ws://"):
        mqtt_broker = f"tcp://{mqtt_broker}"
        
    env["MQTT_BROKER"] = mqtt_broker
    env["MQTT_USER"] = entry.data.get("mqtt_user", "")
    env["MQTT_PASSWORD"] = entry.data.get("mqtt_password", "")
    
    _LOGGER.info(f"Запуск Go-моста ГПУ: {bin_path} (MQTT: {mqtt_broker})")
    
    go_process = subprocess.Popen(
        [bin_path],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    # Регистрация боковой панели
    web_url = entry.data.get("web_url", "http://localhost:8080")
    try:
        frontend.async_register_built_in_panel(
            hass,
            component_name="iframe",
            sidebar_title="Панель ГПУ",
            sidebar_icon="mdi:engine-outline",
            frontend_url_path="gpu_modbus_panel",
            config={"url": web_url},
            require_admin=False
        )
        _LOGGER.info(f"Панель ГПУ зарегистрирована по адресу: {web_url}")
    except Exception as e:
        _LOGGER.error(f"Ошибка регистрации боковой панели: {e}")

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global go_process
    
    try:
        frontend.async_remove_panel(hass, "gpu_modbus_panel")
    except Exception as e:
        _LOGGER.error(f"Ошибка при удалении панели ГПУ: {e}")

    if go_process:
        _LOGGER.info("Остановка Go-моста ГПУ...")
        go_process.terminate()
        go_process.wait()
        go_process = None
    return True