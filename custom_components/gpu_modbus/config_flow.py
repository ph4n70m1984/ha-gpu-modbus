from homeassistant import config_entries
from homeassistant.data_entry_flow import FlowResult
import voluptuous as vol

DOMAIN = "gpu_modbus"

class GpuModbusConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Обработчик мастера настройки для ГПУ Modbus Моста."""
    
    VERSION = 1

    async def async_step_user(self, user_input=None) -> FlowResult:
        """Первый шаг при добавлении интеграции."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        if user_input is not None:
            # Сохраняем введенные пользователем настройки URL и MQTT
            return self.async_create_entry(title="ГПУ Modbus Мост", data=user_input)

        # Форма запроса параметров у пользователя
        data_schema = vol.Schema({
            vol.Required("web_url", default="http://localhost:8080"): str,
            vol.Required("mqtt_broker", default="127.0.0.1:1883"): str,
            vol.Optional("mqtt_user", default=""): str,
            vol.Optional("mqtt_password", default=""): str,
        })

        return self.async_show_form(
            step_id="user", 
            data_schema=data_schema,
            description_placeholders={
                "info": "Настройте параметры MQTT-брокера и доступ к веб-панели."
            }
        )