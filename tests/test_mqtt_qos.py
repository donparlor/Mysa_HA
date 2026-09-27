"""Regressions for MQTT QoS 1 on the shared persistent connection."""

import asyncio
import json
import struct
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import websockets.exceptions  # noqa: F401 -- initialize closure exception types

from custom_components.mysa import mqtt
from custom_components.mysa.realtime import MysaRealtime


async def run_frames(realtime, ws, frames):
    """Run the real receive loop over wire frames, then stop deterministically."""
    # Match what _mqtt_listen() does on connect, so the silence watchdog
    # cannot fire while the loop drains the supplied frames.
    now = time.time()
    realtime._last_packet_time = now
    realtime._last_data_time = now
    ws.recv.side_effect = [*frames, asyncio.CancelledError()]
    with pytest.raises(asyncio.CancelledError):
        await realtime._run_mqtt_loop(ws)


async def test_concurrent_device_polls_have_distinct_packet_ids():
    """Twelve devices must not share one unacknowledged QoS 1 identifier."""
    realtime = MysaRealtime(MagicMock(), AsyncMock(), AsyncMock())
    ws = AsyncMock()
    realtime._mqtt_ws = ws
    await asyncio.gather(
        *(
            realtime.send_command(
                f"device{index}",
                {"MsgType": 11, "Timeout": 300},
                "user",
                msg_type=11,
                wrap=False,
                use_persistent_only=True,
            )
            for index in range(12)
        )
    )
    await realtime.publish("$aws/things/device/shadow/get", {})
    packets = [mqtt.parse_one(call.args[0]) for call in ws.send.call_args_list]
    assert len(packets) == 13
    assert all(packet.qos == 1 for packet in packets)
    assert len({packet.packetid for packet in packets}) == 13
    assert all(1 <= packet.packetid <= 65535 for packet in packets)


@pytest.mark.parametrize("qos", [0, 1])
async def test_incoming_publish_acknowledged_and_delivered(qos):
    """Acknowledge QoS 1 (including redelivery), while delivering telemetry."""
    update = AsyncMock()
    realtime = MysaRealtime(MagicMock(), AsyncMock(), update)
    ws = AsyncMock()
    frames = [
        mqtt.publish(
            "/v1/dev/device/out",
            dup,
            qos,
            False,
            packet_id=321 if qos else None,
            payload=json.dumps({"MsgType": 0, "MainTemp": 20.29}).encode(),
        )
        for dup in [False, True]
    ]
    await run_frames(realtime, ws, frames)
    assert update.await_count == 2
    if qos:
        assert [call.args[0] for call in ws.send.call_args_list] == [
            b"\x40\x02\x01\x41",
            b"\x40\x02\x01\x41",
        ]
    else:
        ws.send.assert_not_awaited()


async def test_puback_releases_only_the_acknowledged_identifier():
    """Release only the matching reservation when PUBACK arrives."""
    realtime = MysaRealtime(MagicMock(), AsyncMock(), AsyncMock())
    ws = AsyncMock()
    realtime._mqtt_ws = ws
    await realtime.send_command("a", {}, "user")
    await realtime.publish("topic", {})
    first, second = [mqtt.parse_one(c.args[0]).packetid for c in ws.send.call_args_list]
    await run_frames(realtime, ws, [struct.pack("!BBH", 0x40, 2, first)])
    assert first not in realtime._pending_publish_ids
    assert second in realtime._pending_publish_ids
    # A repeated PUBACK must be harmless.
    await run_frames(realtime, ws, [struct.pack("!BBH", 0x40, 2, first)])


def test_identifier_wrap_skips_inflight_packets():
    """Wrap identifiers without reusing outstanding publishes."""
    realtime = MysaRealtime(MagicMock(), AsyncMock(), AsyncMock())
    realtime._last_publish_id = 65534
    realtime._pending_publish_ids = {65535, 1}
    assert realtime._allocate_publish_id() == 2
    assert realtime._pending_publish_ids == {65535, 1, 2}


def test_identifier_exhaustion_does_not_reuse_inflight_packet():
    """Fail rather than violate MQTT when every identifier is reserved."""
    realtime = MysaRealtime(MagicMock(), AsyncMock(), AsyncMock())
    realtime._pending_publish_ids = set(range(1, 65536))
    with pytest.raises(RuntimeError, match="packet identifiers"):
        realtime._allocate_publish_id()


async def test_new_connection_clears_pending_identifiers():
    """Discard reservations when opening a clean session."""
    realtime = MysaRealtime(
        MagicMock(), AsyncMock(return_value="wss://test"), AsyncMock()
    )
    realtime._pending_publish_ids = {7, 10}
    ws = AsyncMock()
    with (
        patch("custom_components.mysa.realtime.connect_websocket", return_value=ws),
        patch.object(realtime, "_perform_mqtt_handshake", new_callable=AsyncMock),
        patch.object(realtime, "_run_mqtt_loop", new_callable=AsyncMock),
    ):
        await realtime._mqtt_listen()
    assert not realtime._pending_publish_ids


async def test_receive_loop_recovers_after_packet_parse_error():
    """A failed packet parse must not prevent later telemetry and its ACK."""
    update = AsyncMock()
    realtime = MysaRealtime(MagicMock(), AsyncMock(), update)
    ws = AsyncMock()
    frame = mqtt.publish(
        "/v1/dev/device/out",
        False,
        1,
        False,
        packet_id=321,
        payload=json.dumps({"MsgType": 0, "MainTemp": 20.29}).encode(),
    )
    with patch(
        "custom_components.mysa.realtime.parse_mqtt_packet",
        side_effect=[ValueError("Malformed packet"), mqtt.parse_one(frame)],
    ):
        await run_frames(realtime, ws, [b"bad", frame])
    update.assert_awaited_once()
    ws.send.assert_awaited_once_with(b"\x40\x02\x01\x41")
