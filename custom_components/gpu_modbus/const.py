"""Константы для gpu_modbus."""

DOMAIN = "gpu_modbus"

CONF_DEVICE_ID = "device_id"
CONF_DEVICE_NAME = "device_name"
CONF_HOST = "host"
CONF_PORT = "port"
CONF_MODEL_ID = "model_id"
CONF_TIMEOUT_SEC = "timeout_sec"
CONF_ANOMALY_WINDOW = "anomaly_window"
CONF_ANOMALY_SENSITIVITY = "anomaly_sensitivity"
CONF_VM_WRITE_URL = "vm_write_url"

# Параметры MQTT
CONF_MQTT_BROKER = "mqtt_broker"
CONF_MQTT_USER = "mqtt_user"
CONF_MQTT_PASSWORD = "mqtt_password"

DEFAULT_PORT = 502
DEFAULT_TIMEOUT_SEC = 10
DEFAULT_ANOMALY_WINDOW = 60
DEFAULT_ANOMALY_SENSITIVITY = 3.0
DEFAULT_MQTT_BROKER = "tcp://core-mosquitto:1883"