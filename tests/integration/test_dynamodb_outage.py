"""DynamoDB Local backs message persistence, room membership, and unread
tracking. Left with boto3's stock defaults, a call made while it's down
hangs for a long time rather than failing fast, and — because it used to
run with `-inMemory` — an ordinary restart would silently wipe every
table's schema and data along with it, breaking every gateway (even for
operations unrelated to whatever triggered the outage) until each was
manually restarted. Fixed on three fronts: tight boto3 timeouts so calls
fail fast, a persistent volume so an ordinary restart doesn't lose data,
and a self-heal path (recreate tables + retry once) if they're ever found
missing anyway.
"""

import asyncio
import json
import time
import uuid

import httpx
import pytest
import websockets

from tests.integration.conftest import http_url, ws_url

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


async def test_connection_survives_dynamo_outage_and_fails_fast(dynamodb_outage):
    room_id = f"dynamo-outage-{uuid.uuid4().hex[:8]}"
    alice = await _connect_and_join(f"alice-{uuid.uuid4().hex[:8]}", room_id)

    dynamodb_outage.stop()
    try:
        await asyncio.sleep(1)  # let the outage actually take effect

        # A DynamoDB failure during the outage must surface as a clean
        # error frame, quickly — not the multi-second-plus hang boto3's
        # stock timeouts/retries would otherwise produce, and not a torn
        # down connection either.
        t0 = time.perf_counter()
        ack = await asyncio.wait_for(_send(alice, room_id, "during outage"), timeout=10)
        elapsed = time.perf_counter() - t0

        assert ack["type"] == "error"
        assert ack["error"] == "dynamo_unavailable"
        assert elapsed < 8, f"took {elapsed:.1f}s to fail — should be bounded by boto3 timeouts"

        # the socket itself must still be open
        await alice.send(json.dumps({"type": "ping"}))
        pong = json.loads(await asyncio.wait_for(alice.recv(), timeout=5))
        assert pong["type"] == "pong"
    finally:
        dynamodb_outage.restore()

    await alice.close()


async def test_messages_and_persistence_recover_after_dynamo_comes_back(dynamodb_outage):
    room_id = f"dynamo-recovery-{uuid.uuid4().hex[:8]}"
    alice = await _connect_and_join(f"alice-{uuid.uuid4().hex[:8]}", room_id)
    bob = await _connect_and_join(f"bob-{uuid.uuid4().hex[:8]}", room_id)

    # sanity check: sending works and is durable before the outage
    before_ack = await _send(alice, room_id, "before outage")
    assert before_ack["type"] == "sent"
    await asyncio.wait_for(alice.recv(), timeout=5)  # echo
    await asyncio.wait_for(bob.recv(), timeout=5)  # delivered

    dynamodb_outage.stop()
    await asyncio.sleep(1)

    ack = await asyncio.wait_for(_send(alice, room_id, "during outage"), timeout=10)
    assert ack["type"] == "error"

    dynamodb_outage.restore()

    # DynamoDB Local's Java process takes a beat to actually accept
    # connections again even once the container reports healthy, so retry
    # rather than expecting the very first attempt after restart to land.
    sent_ack = None
    for _ in range(20):
        ack = await asyncio.wait_for(_send(alice, room_id, "after outage"), timeout=5)
        if ack["type"] == "sent":
            sent_ack = ack
            await asyncio.wait_for(alice.recv(), timeout=2)  # echo
            await asyncio.wait_for(bob.recv(), timeout=2)  # delivered
            break
        await asyncio.sleep(0.5)

    assert sent_ack is not None, "sends never recovered after the DynamoDB outage"

    # and the message from before the outage is still there — the
    # persistent volume (not -inMemory) survived the restart
    async with httpx.AsyncClient() as client:
        history = (await client.get(http_url(f"/rooms/{room_id}/messages"))).json()
    texts = [m["text"] for m in history["messages"]]
    assert "before outage" in texts
    assert "after outage" in texts

    await asyncio.gather(alice.close(), bob.close())
