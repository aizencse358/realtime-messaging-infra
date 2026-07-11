import asyncio
import json
import uuid

import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from tests.integration.conftest import GATEWAY_SERVICES, ws_url

pytestmark = pytest.mark.integration


async def _connect(user_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    connected = json.loads(await ws.recv())
    return ws, connected["gateway_id"]


def _service_for_gateway_id(gateway_id: str) -> str:
    # gateway_id is "{GATEWAY_ID env value}-{random suffix}", e.g.
    # "gateway2-6cee42cc" -> compose service "gateway2".
    for service in GATEWAY_SERVICES:
        if gateway_id.startswith(service):
            return service
    raise ValueError(f"couldn't map gateway_id {gateway_id!r} to a compose service")


async def test_killing_a_gateway_drops_its_connections_but_cluster_stays_up(
    restart_gateway_after,
):
    user_id = f"resilience-{uuid.uuid4().hex[:8]}"
    ws, gateway_id = await _connect(user_id)
    service = _service_for_gateway_id(gateway_id)

    # `docker compose stop` sends SIGTERM; uvicorn's graceful shutdown closes
    # open connections rather than leaving them to time out.
    restart_gateway_after(service)

    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(ws.recv(), timeout=15)

    # nginx should route around the now-down replica automatically (default
    # proxy_next_upstream behavior) — new connections must keep working,
    # and none of them should land on the replica we just killed.
    new_sockets, new_gateway_ids = [], []
    try:
        for i in range(6):
            new_ws, new_gw = await _connect(f"after-kill-{i}-{uuid.uuid4().hex[:6]}")
            new_sockets.append(new_ws)
            new_gateway_ids.append(new_gw)

        assert all(not gw.startswith(service) for gw in new_gateway_ids), new_gateway_ids
    finally:
        await asyncio.gather(*[s.close() for s in new_sockets])


async def test_room_still_fans_out_after_a_replica_is_killed(restart_gateway_after):
    room_id = f"post-kill-{uuid.uuid4().hex[:8]}"

    victim_ws, victim_gateway_id = await _connect(f"victim-{uuid.uuid4().hex[:8]}")
    service = _service_for_gateway_id(victim_gateway_id)
    restart_gateway_after(service)
    with pytest.raises(ConnectionClosed):
        await asyncio.wait_for(victim_ws.recv(), timeout=15)

    # after the kill, two fresh connections should still be able to join a
    # room and fan a message out to each other through the surviving
    # replicas + Redis.
    alice, _ = await _connect(f"alice-{uuid.uuid4().hex[:8]}")
    bob, _ = await _connect(f"bob-{uuid.uuid4().hex[:8]}")
    try:
        await alice.send(json.dumps({"type": "join", "room_id": room_id}))
        await alice.recv()
        await bob.send(json.dumps({"type": "join", "room_id": room_id}))
        await bob.recv()

        await alice.send(
            json.dumps({"type": "send", "room_id": room_id, "text": "still alive"})
        )
        await alice.recv()  # sent ack
        await alice.recv()  # echo

        delivered = json.loads(await asyncio.wait_for(bob.recv(), timeout=5))
        assert delivered["text"] == "still alive"
    finally:
        await asyncio.gather(*[alice.close(), bob.close()])
