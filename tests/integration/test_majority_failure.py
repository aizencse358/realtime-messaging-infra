"""The other chaos tests each kill one thing at a time: one gateway replica
(nginx just routes around it), or one shared dependency (Redis/DynamoDB,
which every gateway depends on equally). This one asks a different
question: what if a *majority* of the gateway fleet is down at once —
2 of 3 replicas — leaving a single survivor to carry the entire load?
Unlike the Redis/DynamoDB cases, this isn't about self-healing a broken
connection; it's about confirming the system doesn't have some non-obvious
failure mode once concentrated onto one instance (e.g. the refcounted
subscription logic, or fanout, assuming more than one local instance is
ever involved).
"""

import asyncio
import json
import time
import uuid

import pytest
import websockets

from tests.integration.conftest import GATEWAY_SERVICES, docker_compose, ws_url

pytestmark = pytest.mark.integration

SURVIVOR = "gateway3"
KILLED = [s for s in GATEWAY_SERVICES if s != SURVIVOR]


async def _connect_and_join(user_id: str, room_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    connected = json.loads(await ws.recv())
    await ws.send(json.dumps({"type": "join", "room_id": room_id}))
    joined = json.loads(await ws.recv())
    assert joined["type"] == "joined"
    return ws, connected["gateway_id"]


async def test_single_survivor_keeps_serving_and_fanning_out(request):
    for service in KILLED:
        docker_compose("stop", service)

    def _restore():
        for service in KILLED:
            docker_compose("start", service)
        # nginx's fail_timeout keeps just-recovered replicas out of
        # rotation for a while; give the stack a moment before any later
        # test relies on all 3 being back in the pool.
        time.sleep(12)

    request.addfinalizer(_restore)

    room_id = f"majority-failure-{uuid.uuid4().hex[:8]}"

    # every new connection has nowhere to go but the survivor
    sockets, gateway_ids = [], []
    for i in range(6):
        ws, gw = await _connect_and_join(f"user{i}-{uuid.uuid4().hex[:6]}", room_id)
        sockets.append(ws)
        gateway_ids.append(gw)

    assert all(gw.startswith(SURVIVOR) for gw in gateway_ids), gateway_ids

    # and it still fans messages out correctly among them
    sender, others = sockets[0], sockets[1:]
    await sender.send(
        json.dumps({"type": "send", "room_id": room_id, "text": "still alive on one replica"})
    )
    await sender.recv()  # sent ack
    await sender.recv()  # echo

    received = await asyncio.gather(*[asyncio.wait_for(ws.recv(), timeout=5) for ws in others])
    for raw in received:
        frame = json.loads(raw)
        assert frame["type"] == "message"
        assert frame["text"] == "still alive on one replica"

    await asyncio.gather(*[s.close() for s in sockets])


async def test_load_spreads_across_all_replicas_again_after_recovery(request):
    def _restore():
        for service in KILLED:
            docker_compose("start", service)

    # safety net in case the test fails before the explicit restore below;
    # idempotent (docker compose start on an already-running container is
    # a no-op), so it's harmless to also call it mid-test.
    request.addfinalizer(_restore)

    for service in KILLED:
        docker_compose("stop", service)

    # confirm the majority-failure state actually took effect first
    room_id = f"majority-recovery-{uuid.uuid4().hex[:8]}"
    ws, gw = await _connect_and_join(f"during-{uuid.uuid4().hex[:6]}", room_id)
    assert gw.startswith(SURVIVOR)
    await ws.close()

    _restore()
    # give the recovering replicas a moment to finish startup (DynamoDB
    # table check, Redis pub/sub connect) and nginx's fail_timeout window
    # to expire so they're back in the round-robin rotation.
    time.sleep(12)

    sockets, gateway_ids = [], []
    for i in range(9):
        ws, gw = await _connect_and_join(f"after-{i}-{uuid.uuid4().hex[:6]}", room_id)
        sockets.append(ws)
        gateway_ids.append(gw)

    try:
        assert len(set(gateway_ids)) > 1, gateway_ids
    finally:
        await asyncio.gather(*[s.close() for s in sockets])
