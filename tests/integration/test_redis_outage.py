"""Redis is the one dependency every gateway shares — unlike a dead gateway
replica (which nginx just routes around) or a failed DynamoDB write (which
only affects one message), a Redis outage hits fanout/presence/registry on
all 3 replicas at once. This is the scenario that surfaced a real gap while
building this project: the dedicated pub/sub connection doesn't reconnect
itself the way redis-py's regular connection pool does, so without explicit
handling a Redis restart would silently and *permanently* kill fanout on
every gateway until each was manually restarted.
"""

import asyncio
import json
import uuid

import pytest
import websockets

from tests.integration.conftest import ws_url

pytestmark = pytest.mark.integration


async def _connect_and_join(user_id: str, room_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    await ws.recv()  # connected frame
    await ws.send(json.dumps({"type": "join", "room_id": room_id}))
    await ws.recv()  # joined ack
    return ws


async def _send(ws, room_id: str, text: str) -> dict:
    await ws.send(json.dumps({"type": "send", "room_id": room_id, "text": text}))
    return json.loads(await ws.recv())


async def test_connection_survives_redis_outage_instead_of_closing(redis_outage):
    room_id = f"redis-outage-{uuid.uuid4().hex[:8]}"
    alice = await _connect_and_join(f"alice-{uuid.uuid4().hex[:8]}", room_id)

    redis_outage.stop()
    try:
        await asyncio.sleep(1)  # let the outage actually take effect

        # A publish failure during the outage must surface as an error
        # frame, not tear down the connection — this is the behavior that
        # regressed to a hard ConnectionClosedError before the fix.
        ack = await asyncio.wait_for(_send(alice, room_id, "during outage"), timeout=5)
        assert ack["type"] == "error"
        assert ack["error"] == "redis_unavailable"

        # the socket itself must still be open — a further ping should work
        await alice.send(json.dumps({"type": "ping"}))
        pong = json.loads(await asyncio.wait_for(alice.recv(), timeout=5))
        assert pong["type"] == "pong"
    finally:
        redis_outage.restore()

    await alice.close()


async def test_fanout_recovers_automatically_after_redis_comes_back(redis_outage):
    room_id = f"redis-recovery-{uuid.uuid4().hex[:8]}"
    alice = await _connect_and_join(f"alice-{uuid.uuid4().hex[:8]}", room_id)
    bob = await _connect_and_join(f"bob-{uuid.uuid4().hex[:8]}", room_id)

    # sanity check: fanout works before the outage
    await _send(alice, room_id, "before outage")
    await asyncio.wait_for(alice.recv(), timeout=5)  # echo
    delivered = json.loads(await asyncio.wait_for(bob.recv(), timeout=5))
    assert delivered["text"] == "before outage"

    redis_outage.stop()
    await asyncio.sleep(1)

    # confirm the outage is actually visible before asserting recovery
    ack = await asyncio.wait_for(_send(alice, room_id, "during outage"), timeout=5)
    assert ack["type"] == "error"

    redis_outage.restore()

    # Each gateway's listener reconnects and resubscribes with backoff once
    # Redis is reachable again — there's a real window right after restart
    # where a publish can race ahead of resubscription and simply not be
    # delivered (normal Redis pub/sub semantics, not a bug), so retry until
    # a full round trip succeeds rather than expecting the very first
    # attempt after restart to land.
    delivered = None
    for attempt in range(20):
        ack = await asyncio.wait_for(_send(alice, room_id, f"after outage {attempt}"), timeout=5)
        if ack["type"] != "sent":
            await asyncio.sleep(0.5)
            continue
        try:
            await asyncio.wait_for(alice.recv(), timeout=1)  # echo
            delivered = json.loads(await asyncio.wait_for(bob.recv(), timeout=1))
            break
        except asyncio.TimeoutError:
            await asyncio.sleep(0.5)
            continue

    assert delivered is not None, "fanout never recovered after the Redis outage"
    assert delivered["text"].startswith("after outage")

    await asyncio.gather(alice.close(), bob.close())
