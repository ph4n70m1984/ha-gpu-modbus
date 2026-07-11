import os
import platform
import asyncio
import logging
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.components import frontend

_LOGGER = logging.getLogger(__name__)
DOMAIN = "gpu_modbus"

go_process = None
log_task = None

async def _read_stream(stream, log_func):
    """Асинхронное чтение потока вывода без блокировки."""
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            decoded_line = line.decode('utf-8', errors='replace').strip()
            if decoded_line:
                log_func(f"[Go Backend] {decoded_line}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        _LOGGER.error(f"Ошибка при чтении журнала Go-моста: {e}")

async def start_go_process(bin_path, env, hass):
    """Запуск бинарника как полностью независимого фонового процесса."""
    global go_process
    try:
        # Используем нативный asyncio вместо subprocess, чтобы не блокировать event loop
        go_process = await asyncio.create_subprocess_exec(
            bin_path,
            env=env,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        # Запускаем чтение логов 
        t1 = hass.loop.create_task(_read_stream(go_process.stdout, _LOGGER.info))
        t2 = hass.loop.create_task(_read_stream(go_process.stderr, _LOGGER.warning))
        
        await asyncio.gather(t1, t2)
        
    except asyncio.CancelledError:
        if go_process:
            _LOGGER.info("Отмена фоновой задачи: завершение Go-процесса...")
            try:
                go_process.terminate()
            except ProcessLookupError:
                pass
    except Exception as e:
        _LOGGER.error(f"Критическая ошибка работы Go-процесса: {e}")

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global log_task
    
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

    # Подготовка переменных окружения
    env = os.environ.copy()
    env["CONTROLLERS_PATH"] = os.path.join(current_dir, "controllers")
    env["WEB_PATH"] = os.path.join(current_dir, "web")
    env["DATA_PATH"] = os.path.join(hass.config.config_dir, "gpu_modbus_data")
    
    mqtt_broker = entry.data.get("mqtt_broker", "127.0.0.1:1883")
    if not mqtt_broker.startswith("tcp://") and not mqtt_broker.startswith("ws://"):
        mqtt_broker = f"tcp://{mqtt_broker}"
        
    env["MQTT_BROKER"] = mqtt_broker
    env["MQTT_USER"] = entry.data.get("mqtt_user", "")
    env["MQTT_PASSWORD"] = entry.data.get("mqtt_password", "")
    
    _LOGGER.info(f"Инициализация Go-моста ГПУ: {bin_path}")
    
    # ЗАЩИТА ОТ ЗАВИСАНИЯ HA:
    # Запускаем процесс как фоновую задачу (Background Task), 
    # чтобы ядро Home Assistant не ждало её завершения при старте системы.
    if hasattr(hass, "async_create_background_task"):
        log_task = hass.async_create_background_task(
            start_go_process(bin_path, env, hass),
            name="gpu_modbus_go_backend"
        )
    else:
        # Фолбэк для старых версий HA
        log_task = hass.loop.create_task(start_go_process(bin_path, env, hass))

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
    except Exception as e:
        _LOGGER.error(f"Ошибка регистрации боковой панели: {e}")

    return True

async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global log_task, go_process
    
    try:
        frontend.async_remove_panel(hass, "gpu_modbus_panel")
    except Exception as e:
        _LOGGER.error(f"Ошибка при удалении панели ГПУ: {e}")

    # При удалении интеграции корректно отменяем фоновую задачу,
    # что автоматически вызовет CancelledError и убьет процесс Go.
    if log_task:
        log_task.cancel()
        log_task = None
        
    if go_process:
        try:
            go_process.terminate()
        except Exception:
            pass
        go_process = None
        
    return True