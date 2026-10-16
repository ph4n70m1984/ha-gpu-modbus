"""Инициализация интеграции gpu_modbus."""
from __future__ import annotations

import asyncio
import json
import logging
import os
import platform

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import (
    DOMAIN,
    CONF_DEVICE_ID,
    CONF_DEVICE_NAME,
    CONF_HOST,
    CONF_PORT,
    CONF_MODEL_ID,
    CONF_TIMEOUT_SEC,
    CONF_ANOMALY_WINDOW,
    CONF_ANOMALY_SENSITIVITY,
    CONF_VM_WRITE_URL,
    DEFAULT_TIMEOUT_SEC,
    DEFAULT_ANOMALY_WINDOW,
    DEFAULT_ANOMALY_SENSITIVITY,
)

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Глобальная инициализация интеграции."""
    hass.data.setdefault(DOMAIN, {
        "process": None,
        "entries": {},
    })
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Настройка записи конфигурации из UI."""
    data = hass.data[DOMAIN]
    data["entries"][entry.entry_id] = entry

    # Подготавливаем рабочие папки
    storage_dir = hass.config.path(".storage", DOMAIN)
    custom_controllers_dir = hass.config.path("gpu_modbus_custom")
    os.makedirs(storage_dir, exist_ok=True)
    os.makedirs(custom_controllers_dir, exist_ok=True)

    # Обновляем user_devices.json для Go-демона
    await hass.async_add_executor_job(sync_devices_json, hass)

    # Если процесс моста еще не запущен — запускаем
    if data["process"] is None:
        await start_bridge_process(hass, entry)

    # Регистрируем слушатель обновления настроек (Options)
    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Удаление записи ГПУ."""
    data = hass.data[DOMAIN]
    if entry.entry_id in data["entries"]:
        del data["entries"][entry.entry_id]

    await hass.async_add_executor_job(sync_devices_json, hass)

    # Если устройств не осталось — глушим фоновый процесс
    if not data["entries"] and data["process"] is not None:
        proc = data["process"]
        try:
            proc.terminate()
            await proc.wait()
        except Exception as e:
            _LOGGER.warning("Ошибка остановки gpu_bridge: %s", e)
        data["process"] = None

    return True


async def async_reload_entry(hass: HomeAssistant, entry: ConfigEntry) -> None:
    """Перезагрузка при изменении настроек в UI."""
    await async_unload_entry(hass, entry)
    await async_setup_entry(hass, entry)


def sync_devices_json(hass: HomeAssistant):
    """Синхронизирует ConfigEntries в user_devices.json с поддержкой старых конфигураций."""
    data = hass.data[DOMAIN]
    storage_dir = hass.config.path(".storage", DOMAIN)
    json_path = os.path.join(storage_dir, "user_devices.json")

    # Читаем уже существующие устройства, если они были созданы ранее
    existing_devices: dict[str, dict] = {}
    legacy_paths = [
        json_path,
        hass.config.path("data", "user_devices.json"),
    ]
    for p in legacy_paths:
        if os.path.exists(p):
            try:
                with open(p, "r", encoding="utf-8") as f:
                    for d in json.load(f):
                        dev_id = d.get("id")
                        if dev_id:
                            existing_devices[dev_id] = d
            except Exception as e:
                _LOGGER.warning("Ошибка чтения существующего %s: %s", p, e)

    device_list = []

    for entry in data["entries"].values():
        cfg = {**entry.data, **entry.options}
        
        # Если запись старая и в ней нет поля 'host', пропускаем падение
        host = cfg.get(CONF_HOST)
        if not host:
            _LOGGER.warning("Запись '%s' не содержит хоста (старый формат), пропускаем", entry.title)
            continue

        port = cfg.get(CONF_PORT, 502)
        dev_id = cfg.get(CONF_DEVICE_ID, f"gpu_{host.replace('.', '_')}_{port}")

        device_entry = {
            "id": dev_id,
            "name": cfg.get(CONF_DEVICE_NAME, entry.title),
            "address": f"{host}:{port}",
            "model_id": cfg.get(CONF_MODEL_ID, "generic"),
            "timeout_sec": cfg.get(CONF_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC),
            "anomaly_window": cfg.get(CONF_ANOMALY_WINDOW, DEFAULT_ANOMALY_WINDOW),
            "anomaly_sensitivity": cfg.get(CONF_ANOMALY_SENSITIVITY, DEFAULT_ANOMALY_SENSITIVITY),
        }
        
        device_list.append(device_entry)
        existing_devices[dev_id] = device_entry

    # Если в UI пока нет новых записей с хостом, сохраняем ранее созданные устройства
    final_list = device_list if device_list else list(existing_devices.values())

    os.makedirs(storage_dir, exist_ok=True)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(final_list, f, indent=2, ensure_ascii=False)


async def start_bridge_process(hass: HomeAssistant, entry: ConfigEntry):
    """Запускает Go-бинарник моста без HTTP сервера."""
    arch = platform.machine().lower()
    bin_name = "gpu_bridge_arm64" if ("aarch64" in arch or "arm64" in arch) else "gpu_bridge_amd64"
    bin_path = os.path.join(os.path.dirname(__file__), "bin", bin_name)

    if not os.path.exists(bin_path):
        _LOGGER.error("Бинарный файл моста %s не найден", bin_path)
        raise ConfigEntryNotReady(f"Отсутствует бинарник {bin_name}")

    os.chmod(bin_path, 0o755)

    # Получаем параметры MQTT брокера из Home Assistant
    mqtt_broker = "tcp://127.0.0.1:1883"
    mqtt_user = ""
    mqtt_pass = ""

    # Пробуем автоматически взять реквизиты из штатной интеграции MQTT
    mqtt_entry = hass.config_entries.async_entries("mqtt")
    if mqtt_entry:
        m_data = mqtt_entry[0].data
        m_broker = m_data.get("broker", "127.0.0.1")
        m_port = m_data.get("port", 1883)
        mqtt_broker = f"tcp://{m_broker}:{m_port}"
        mqtt_user = m_data.get("username", "")
        mqtt_pass = m_data.get("password", "")

    cfg = {**entry.data, **entry.options}
    env = os.environ.copy()
    env.update({
        "MQTT_BROKER": mqtt_broker,
        "MQTT_USER": mqtt_user,
        "MQTT_PASSWORD": mqtt_pass,
        "CONTROLLERS_PATH": os.path.join(os.path.dirname(__file__), "controllers"),
        "CUSTOM_CONTROLLERS_PATH": hass.config.path("gpu_modbus_custom"),
        "DATA_PATH": hass.config.path(".storage", DOMAIN),
        "VM_WRITE_URL": cfg.get(CONF_VM_WRITE_URL, ""),
        "DISABLE_HTTP_SERVER": "1",  # Флаг отключения HTTP веб-сервера
    })

    _LOGGER.info("Запуск фонового процесса %s...", bin_name)
    proc = await asyncio.create_subprocess_exec(
        bin_path,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    hass.data[DOMAIN]["process"] = proc