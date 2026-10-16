"""Config flow для интеграции ГПУ Modbus TCP."""
from __future__ import annotations

import os
import glob
import json
import logging
from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
import homeassistant.helpers.config_validation as cv

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
    DEFAULT_PORT,
    DEFAULT_TIMEOUT_SEC,
    DEFAULT_ANOMALY_WINDOW,
    DEFAULT_ANOMALY_SENSITIVITY,
    DEFAULT_MQTT_BROKER,
)

_LOGGER = logging.getLogger(__name__)


def get_available_models(hass) -> dict[str, str]:
    """Сканирует встроенные и пользовательские JSON-файлы моделей контроллеров."""
    models: dict[str, str] = {}
    builtin_path = os.path.join(os.path.dirname(__file__), "controllers")
    custom_path = hass.config.path("gpu_modbus_custom")

    for path in (builtin_path, custom_path):
        if not os.path.exists(path):
            continue
        for file_path in glob.glob(os.path.join(path, "*.json")):
            try:
                with open(file_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    model_id = data.get("model_id")
                    name = data.get("name", model_id)
                    if model_id:
                        models[model_id] = f"{name} ({data.get('manufacturer', 'Generic')})"
            except Exception as err:
                _LOGGER.warning("Ошибка чтения файла %s: %s", file_path, err)

    if not models:
        models["generic"] = "Универсальный контроллер (Generic)"
    return models


class GPUModbusConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Мастер добавления устройства ГПУ в Home Assistant."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Шаг добавления новой установки."""
        errors: dict[str, str] = {}
        available_models = await self.hass.async_add_executor_job(get_available_models, self.hass)

        if user_input is not None:
            host = user_input[CONF_HOST]
            port = user_input[CONF_PORT]
            device_id = f"gpu_{host.replace('.', '_')}_{port}"

            await self.async_set_unique_id(device_id)
            self._abort_if_unique_id_configured()

            user_input[CONF_DEVICE_ID] = device_id
            return self.async_create_entry(
                title=user_input[CONF_DEVICE_NAME],
                data=user_input,
            )

        data_schema = vol.Schema(
            {
                vol.Required(CONF_DEVICE_NAME, default="ГПУ Генератор 1"): cv.string,
                vol.Required(CONF_HOST): cv.string,
                vol.Required(CONF_PORT, default=DEFAULT_PORT): cv.port,
                vol.Required(CONF_MODEL_ID): vol.In(available_models),
                vol.Required(CONF_MQTT_BROKER, default=DEFAULT_MQTT_BROKER): cv.string,
                vol.Optional(CONF_MQTT_USER, default=""): cv.string,
                vol.Optional(CONF_MQTT_PASSWORD, default=""): cv.string,
                vol.Optional(CONF_TIMEOUT_SEC, default=DEFAULT_TIMEOUT_SEC): vol.All(
                    vol.Coerce(int), vol.Range(min=2, max=60)
                ),
                vol.Optional(CONF_ANOMALY_WINDOW, default=DEFAULT_ANOMALY_WINDOW): vol.All(
                    vol.Coerce(int), vol.Range(min=20, max=300)
                ),
                vol.Optional(
                    CONF_ANOMALY_SENSITIVITY, default=DEFAULT_ANOMALY_SENSITIVITY
                ): vol.All(vol.Coerce(float), vol.Range(min=1.0, max=10.0)),
                vol.Optional(CONF_VM_WRITE_URL, default=""): cv.string,
            }
        )

        return self.async_show_form(
            step_id="user", data_schema=data_schema, errors=errors
        )

    @staticmethod
    @callback
    def async_get_options_flow(
        config_entry: config_entries.ConfigEntry,
    ) -> config_entries.OptionsFlow:
        """Создает обработчик меню 'Настроить' (Options Flow)."""
        return GPUModbusOptionsFlowHandler(config_entry)


class GPUModbusOptionsFlowHandler(config_entries.OptionsFlow):
    """Меню изменения настроек существующей ГПУ."""

    def __init__(self, config_entry: config_entries.ConfigEntry) -> None:
        self._config_entry = config_entry

    async def async_step_init(
        self, user_input: dict[str, Any] | None = None
    ) -> FlowResult:
        """Управление параметрами работы."""
        available_models = await self.hass.async_add_executor_job(
            get_available_models, self.hass
        )

        if user_input is not None:
            return self.async_create_entry(title="", data=user_input)

        entry = getattr(self, "config_entry", self._config_entry)
        current = {**entry.data, **entry.options}

        options_schema = vol.Schema(
            {
                vol.Required(CONF_HOST, default=current.get(CONF_HOST)): cv.string,
                vol.Required(CONF_PORT, default=current.get(CONF_PORT, DEFAULT_PORT)): cv.port,
                vol.Required(
                    CONF_MODEL_ID, default=current.get(CONF_MODEL_ID)
                ): vol.In(available_models),
                vol.Required(
                    CONF_MQTT_BROKER,
                    default=current.get(CONF_MQTT_BROKER, DEFAULT_MQTT_BROKER),
                ): cv.string,
                vol.Optional(
                    CONF_MQTT_USER,
                    default=current.get(CONF_MQTT_USER, ""),
                ): cv.string,
                vol.Optional(
                    CONF_MQTT_PASSWORD,
                    default=current.get(CONF_MQTT_PASSWORD, ""),
                ): cv.string,
                vol.Optional(
                    CONF_TIMEOUT_SEC,
                    default=current.get(CONF_TIMEOUT_SEC, DEFAULT_TIMEOUT_SEC),
                ): vol.All(vol.Coerce(int), vol.Range(min=2, max=60)),
                vol.Optional(
                    CONF_ANOMALY_WINDOW,
                    default=current.get(CONF_ANOMALY_WINDOW, DEFAULT_ANOMALY_WINDOW),
                ): vol.All(vol.Coerce(int), vol.Range(min=20, max=300)),
                vol.Optional(
                    CONF_ANOMALY_SENSITIVITY,
                    default=current.get(
                        CONF_ANOMALY_SENSITIVITY, DEFAULT_ANOMALY_SENSITIVITY
                    ),
                ): vol.All(vol.Coerce(float), vol.Range(min=1.0, max=10.0)),
                vol.Optional(
                    CONF_VM_WRITE_URL,
                    default=current.get(CONF_VM_WRITE_URL, ""),
                ): cv.string,
            }
        )

        return self.async_show_form(step_id="init", data_schema=options_schema)