import os
import platform
import subprocess
import asyncio
import logging
from homeassistant.core import HomeAssistant
from homeassistant.config_entries import ConfigEntry
from homeassistant.components import frontend

_LOGGER = logging.getLogger(__name__)
DOMAIN = "gpu_modbus"

# Глобальные переменные для управления процессом
go_process = None
log_task = None

async def _read_stream(stream, log_func):
    """Фоновая задача для чтения логов из потока Go-бинарника."""
    try:
        while True:
            line = await stream.readline()
            if not line:
                break
            # Декодируем строку и убираем лишние пробелы/переносы
            decoded_line = line.decode('utf-8', errors='replace').strip()
            if decoded_line:
                log_func(f"[Go Backend] {decoded_line}")
    except asyncio.CancelledError:
        pass
    except Exception as e:
        _LOGGER.error(f"Ошибка при чтении журнала Go-моста: {e}")

async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    global go_process, log_task
    
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
    
    # Настройки MQTT
    mqtt_broker = entry.data.get("mqtt_broker", "127.0.0.1:1883")
    if not mqtt_broker.startswith("tcp://") and not mqtt_broker.startswith("ws://"):
        mqtt_broker = f"tcp://{mqtt_broker}"
        
    env["MQTT_BROKER"] = mqtt_broker
    env["MQTT_USER"] = entry.data.get("mqtt_user", "")
    env["MQTT_PASSWORD"] = entry.data.get("mqtt_password", "")
    
    _LOGGER.info(f"Запуск Go-моста ГПУ: {bin_path} (MQTT: {mqtt_broker})")
    
    # Запускаем процесс с перенаправлением stdout и stderr в PIPE
    try:
        go_process = subprocess.Popen(
            [bin_path],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        
        # Переводим потоки в асинхронный режим Home Assistant
        loop = asyncio.get_running_loop()
        
        async def connect_streams():
            reader_out = asyncio.StreamReader()
            protocol_out = asyncio.StreamReaderProtocol(reader_out)
            await loop.connect_read_pipe(lambda: protocol_out, go_process.stdout)
            
            reader_err = asyncio.StreamReader()
            protocol_err = asyncio.StreamReaderProtocol(reader_err)
            await loop.connect_read_pipe(lambda: protocol_err, go_process.stderr)
            
            # ИСПРАВЛЕНИЕ: Просто регистрируем фоновые задачи через hass.async_create_task 
            # и возвращаем объект gather, НЕ блокируя выполнение через await внутри функции установки
            t1 = hass.async_create_task(_read_stream(reader_out, _LOGGER.info))
            t2 = hass.async_create_task(_read_stream(reader_err, _LOGGER.warning))
            return asyncio.gather(t1, t2)

        # Запускаем подключение труб асинхронно без блокировки основного потока HA
        log_task = hass.async_create_task(connect_streams())

    except Exception as e:
        _LOGGER.error(f"Не удалось запустить Go-процесс моста ГПУ: {e}")
        return False

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
    global go_process, log_task
    
    try:
        frontend.async_remove_panel(hass, "gpu_modbus_panel")
    except Exception as e:
        _LOGGER.error(f"Ошибка при удалении панели ГПУ: {e}")

    # Отменяем задачу логирования
    if log_task:
        log_task.cancel()
        log_task = None

    if go_process:
        _LOGGER.info("Остановка Go-моста ГПУ...")
        go_process.terminate()
        go_process.wait()
        go_process = None
        
    return True