import asyncio
import json
import uuid

import pytest
import websockets

from tests.integration.conftest import ws_url

pytestmark = pytest.mark.integration


async def _connect_and_join(user_id: str, room_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    connected = json.loads(await ws.recv())
    await ws.send(json.dumps({"type": "join", "room_id": room_id}))
    joined = json.loads(await ws.recv())
    assert joined["type"] == "joined"
    return ws, connected["gateway_id"]


async def test_connections_spread_across_gateway_replicas():
    room_id = f"spread-{uuid.uuid4().hex[:8]}"
    sockets, gateway_ids = [], []
    for i in range(6):
        ws, gw = await _connect_and_join(f"user{i}-{uuid.uuid4().hex[:6]}", room_id)
        sockets.append(ws)
        gateway_ids.append(gw)

    try:
        # nginx's default round-robin (no ip_hash) should have scattered
        # these 6 connections across more than one replica.
        assert len(set(gateway_ids)) > 1, gateway_ids
    finally:
        await asyncio.gather(*[s.close() for s in sockets])


async def test_message_fans_out_to_members_on_other_gateway_instances():
    room_id = f"fanout-{uuid.uuid4().hex[:8]}"
    sockets, gateway_ids = [], []
    for i in range(6):
        ws, gw = await _connect_and_join(f"fan{i}-{uuid.uuid4().hex[:6]}", room_id)
        sockets.append(ws)
        gateway_ids.append(gw)

    try:
        sender = sockets[0]
        others = sockets[1:]

        await sender.send(
            json.dumps({"type": "send", "room_id": room_id, "text": "cross-instance hello"})
        )
        await sender.recv()  # sent ack
        await sender.recv()  # echo

        received = await asyncio.gather(
            *[asyncio.wait_for(ws.recv(), timeout=5) for ws in others]
        )
        for raw in received:
            frame = json.loads(raw)
            assert frame["type"] == "message"
            assert frame["text"] == "cross-instance hello"

        # confirm at least one recipient was actually on a *different*
        # gateway instance than the sender — otherwise this would only be
        # proving same-instance delivery, not cross-instance pub/sub fanout
        recipient_gateways = gateway_ids[1:]
        assert any(gw != gateway_ids[0] for gw in recipient_gateways), gateway_ids
    finally:
        await asyncio.gather(*[s.close() for s in sockets])
