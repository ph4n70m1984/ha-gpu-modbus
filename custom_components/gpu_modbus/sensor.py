"""Sensor platform stub for HACS compliance."""
import logging

_LOGGER = logging.getLogger(__name__)

async def async_setup_entry(hass, config_entry, async_add_entities):
    """Go binary handles sensors via MQTT Discovery directly."""
    pass