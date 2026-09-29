"""Exercise MQTT decoding through real coordinator listeners and HA states."""

import asyncio
import json
import time
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.const import Platform
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.setup import async_setup_component

from custom_components.mysa import mqtt
from custom_components.mysa.mysa_api import MysaApi


@pytest.mark.parametrize("model", ["BB-V1-0", "AC-V1-0"])
@pytest.mark.parametrize("envelope", ["root", "body", "body_state"])
@pytest.mark.parametrize("temperature_key", ["MainTemp", "ambTemp"])
async def test_mqtt_updates_ha_states(
    hass, mock_config_entry, model, envelope, temperature_key
):
    """A received measurement must reach HA without a manual entity refresh."""
    assert await async_setup_component(hass, "homeassistant", {})
    device_id = "246f28032370"
    api = MysaApi(
        "test@example.com",
        "password",
        hass,
        websession=async_get_clientsession(hass),
    )
    api.devices = {device_id: {"Id": device_id, "Name": "Entry", "Model": model}}

    async def http_state():
        # The HTTP API supplies CorrectedTemp; MQTT supplies MainTemp.
        return api.client._merge_and_normalize_states(
            {
                device_id: {
                    "CorrectedTemp": {"v": 19.0},
                    "Humidity": {"v": 40},
                    "Connected": False,
                    "Mode": 1,
                    "SetPoint": 21,
                    "FirmwareVersion": "1.0",
                    "ip": "192.0.2.1",
                }
            }
        )

    with (
        patch("custom_components.mysa.MysaApi", return_value=api),
        patch(
            "custom_components.mysa.PLATFORMS",
            [
                Platform.SENSOR,
                Platform.CLIMATE,
                Platform.BINARY_SENSOR,
            ],
        ),
        patch.object(api, "authenticate", new_callable=AsyncMock),
        patch.object(api, "get_devices", new_callable=AsyncMock),
        patch.object(api, "start_mqtt_listener", new_callable=AsyncMock),
        patch.object(api, "stop_mqtt_listener", new_callable=AsyncMock),
        patch.object(api.client, "get_state", side_effect=http_state) as poll,
    ):
        assert await hass.config_entries.async_setup(mock_config_entry.entry_id)
        await hass.async_block_till_done()
        registry = er.async_get(hass)
        temperature = registry.async_get_entity_id(
            "sensor", "mysa", f"{device_id}_temperature"
        )
        humidity = registry.async_get_entity_id(
            "sensor", "mysa", f"{device_id}_humidity"
        )
        climate = registry.async_get_entity_id("climate", "mysa", device_id)
        connection = registry.async_get_entity_id(
            "binary_sensor", "mysa", f"{device_id}_connection"
        )
        assert float(hass.states.get(temperature).state) == 19.0
        assert float(hass.states.get(humidity).state) == 40.0
        assert hass.states.get(connection).state == "off"
        poll.reset_mock()

        measurements = {temperature_key: 20.29, "Humidity": 47, "Connected": True}
        payload = {"MsgType": 0, "Timestamp": int(time.time())}
        if envelope == "root":
            payload.update(measurements)
        elif envelope == "body":
            payload["body"] = measurements
        else:
            payload["body"] = {"state": measurements}
        packet = mqtt.PublishPacket(
            dup=0,
            qos=0,
            retain=0,
            packetid=None,
            topic=f"/v1/dev/{device_id}/out",
            payload=json.dumps(payload).encode(),
        )
        coordinator = mock_config_entry.runtime_data.coordinator
        with patch.object(
            coordinator,
            "async_update_listeners",
            wraps=coordinator.async_update_listeners,
        ) as notify:
            await api.realtime._process_mqtt_publish(packet)
            await hass.async_block_till_done()
            notify.assert_called_once()

        poll.assert_not_called()
        # Check the actual state machine, not just a property or a mocked callback.
        assert float(hass.states.get(temperature).state) == 20.29
        assert float(hass.states.get(humidity).state) == 47.0
        assert hass.states.get(climate).attributes["current_temperature"] == 20.3
        assert hass.states.get(climate).attributes["current_humidity"] == 47.0
        assert hass.states.get(connection).state == "on"

        # A second packet exercises the delayed push and nested v/t values.
        measurements[temperature_key] = {"v": 20.71}
        measurements["Humidity"] = {"v": 48}
        if envelope == "root":
            payload.update(measurements)
        packet.payload = json.dumps(payload).encode()
        await api.realtime._process_mqtt_publish(packet)
        await asyncio.sleep(0.6)
        await hass.async_block_till_done()
        assert float(hass.states.get(temperature).state) == 20.71
        assert float(hass.states.get(humidity).state) == 48.0
        poll.assert_not_called()

        # The real update_entity service must still refresh via HTTP after MQTT.
        await hass.services.async_call(
            "homeassistant", "update_entity", {"entity_id": temperature}, blocking=True
        )
        await hass.async_block_till_done()
        poll.assert_awaited_once()
        assert float(hass.states.get(temperature).state) == 19.0
        assert float(hass.states.get(humidity).state) == 40.0
        assert await hass.config_entries.async_unload(mock_config_entry.entry_id)

async def test_ac_v1_mqtt_hvac_freshness_resets_on_reconnect(hass):
    """AC-V1 HVAC freshness must belong to the current MQTT connection."""
    device_id = "ac_device"

    api = MysaApi(
        "test@example.com",
        "password",
        hass,
        websession=async_get_clientsession(hass),
    )
    api.devices = {
        device_id: {
            "Id": device_id,
            "Name": "Test AC",
            "Model": "AC-V1-0",
        }
    }

    # Avoid the metadata-nudge side effect in _on_mqtt_update().
    api.states[device_id] = {
        "FirmwareVersion": "1.0",
        "ip": "192.0.2.1",
        "ACMode": 4,
        "Mode": 4,
    }

    # First successful MQTT connection.
    api.realtime._connection_generation = 1
    api.realtime._mqtt_ws = object()
    api.realtime._mqtt_connected.set()

    assert api.has_fresh_mqtt_hvac_state(device_id) is False

    await api._on_mqtt_update(device_id, {"md": 6})
    assert api.has_fresh_mqtt_hvac_state(device_id) is True
    assert api._mqtt_hvac_generation[device_id] == 1

    # Connection drops: old telemetry must immediately stop being authoritative.
    api.realtime._mqtt_connected.clear()
    api.realtime._mqtt_ws = None
    assert api.has_fresh_mqtt_hvac_state(device_id) is False

    # Automatic reconnect creates a new MQTT generation.
    api.realtime._connection_generation = 2
    api.realtime._mqtt_ws = object()
    api.realtime._mqtt_connected.set()

    # The generation-1 HVAC state is stale in generation 2.
    assert api.has_fresh_mqtt_hvac_state(device_id) is False

    # A protocol-only MQTT echo must not mark HVAC state fresh.
    await api._on_mqtt_update(device_id, {"Timestamp": int(time.time())})
    assert api.has_fresh_mqtt_hvac_state(device_id) is False

    # A real HVAC mode from generation 2 establishes freshness again.
    await api._on_mqtt_update(device_id, {"md": 1})
    assert api.has_fresh_mqtt_hvac_state(device_id) is True
    assert api._mqtt_hvac_generation[device_id] == 2
