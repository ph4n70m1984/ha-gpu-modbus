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
    CONF_MQTT_BROKER,
    CONF_MQTT_USER,
    CONF_MQTT_PASSWORD,
    DEFAULT_TIMEOUT_SEC,
    DEFAULT_ANOMALY_WINDOW,
    DEFAULT_ANOMALY_SENSITIVITY,
    DEFAULT_MQTT_BROKER,
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

    storage_dir = hass.config.path(".storage", DOMAIN)
    custom_controllers_dir = hass.config.path("gpu_modbus_custom")
    os.makedirs(storage_dir, exist_ok=True)
    os.makedirs(custom_controllers_dir, exist_ok=True)

    await hass.async_add_executor_job(sync_devices_json, hass)

    if data["process"] is None:
        await start_bridge_process(hass, entry)

    entry.async_on_unload(entry.add_update_listener(async_reload_entry))
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Удаление записи ГПУ."""
    data = hass.data[DOMAIN]
    if entry.entry_id in data["entries"]:
        del data["entries"][entry.entry_id]

    await hass.async_add_executor_job(sync_devices_json, hass)

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
    """Синхронизирует текущие ConfigEntries в user_devices.json."""
    data = hass.data[DOMAIN]
    device_list = []

    for entry in data["entries"].values():
        cfg = {**entry.data, **entry.options}
        host = cfg.get(CONF_HOST)
        if not host:
            continue

        port = cfg.get(CONF_PORT, 502)
        device_list.append({
            "id": cfg.get(CONF_DEVICE_ID, f"gpu_{host.replace('.', '_')}_{port}"),
            "name": cfg.get(CONF_DEVICE_NAME, entry.title),
            "address": f"{host}:{port}",
            "model_id": cfg.get(CONF_MODEL_ID, "generic"),
            "timeout_sec": cfg.get(CONF_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC),
            "anomaly_window": cfg.get(CONF_ANOMALY_WINDOW, DEFAULT_ANOMALY_WINDOW),
            "anomaly_sensitivity": cfg.get(CONF_ANOMALY_SENSITIVITY, DEFAULT_ANOMALY_SENSITIVITY),
            "mqtt_broker": cfg.get(CONF_MQTT_BROKER, DEFAULT_MQTT_BROKER),
            "mqtt_user": cfg.get(CONF_MQTT_USER, ""),
            "mqtt_password": cfg.get(CONF_MQTT_PASSWORD, ""),
        })

    json_path = hass.config.path(".storage", DOMAIN, "user_devices.json")
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(device_list, f, indent=2, ensure_ascii=False)


async def start_bridge_process(hass: HomeAssistant, entry: ConfigEntry):
    """Запускает Go-бинарник моста с параметрами MQTT из настроек."""
    arch = platform.machine().lower()
    bin_name = "gpu_bridge_arm64" if ("aarch64" in arch or "arm64" in arch) else "gpu_bridge_amd64"
    bin_path = os.path.join(os.path.dirname(__file__), "bin", bin_name)

    if not os.path.exists(bin_path):
        _LOGGER.error("Бинарный файл моста %s не найден", bin_path)
        raise ConfigEntryNotReady(f"Отсутствует бинарник {bin_name}")

    os.chmod(bin_path, 0o755)

    cfg = {**entry.data, **entry.options}
    broker = cfg.get(CONF_MQTT_BROKER, DEFAULT_MQTT_BROKER)
    user = cfg.get(CONF_MQTT_USER, "")
    password = cfg.get(CONF_MQTT_PASSWORD, "")

    env = os.environ.copy()
    env.update({
        "MQTT_BROKER": broker,
        "MQTT_USER": user,
        "MQTT_PASSWORD": password,
        "CONTROLLERS_PATH": os.path.join(os.path.dirname(__file__), "controllers"),
        "CUSTOM_CONTROLLERS_PATH": hass.config.path("gpu_modbus_custom"),
        "DATA_PATH": hass.config.path(".storage", DOMAIN),
        "VM_WRITE_URL": cfg.get(CONF_VM_WRITE_URL, ""),
        "DISABLE_HTTP_SERVER": "1",
    })

    _LOGGER.info("Запуск фонового процесса %s (MQTT: %s)...", bin_name, broker)
    proc = await asyncio.create_subprocess_exec(
        bin_path,
        env=env,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL,
    )
    hass.data[DOMAIN]["process"] = proc