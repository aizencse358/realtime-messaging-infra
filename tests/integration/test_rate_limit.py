"""The rate limit is enforced in Redis, shared across all 3 gateway
replicas — this is what actually matters to verify against the real
stack rather than a single process: a client can't dodge the limit by
reconnecting and landing on a different replica, since the counter lives
in the one place every gateway reads from.
"""

import asyncio
import json
import uuid

import pytest
import websockets

from tests.integration.conftest import ws_url

pytestmark = pytest.mark.integration

# Matches docker-compose.yml's defaults (RATE_LIMIT_SEND_MAX_MESSAGES=10,
# RATE_LIMIT_SEND_WINDOW_SECONDS=5). If those change, update here too.
MAX_MESSAGES = 10
WINDOW_SECONDS = 5


async def _connect_and_join(user_id: str, room_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    await ws.recv()  # connected frame
    await ws.send(json.dumps({"type": "join", "room_id": room_id}))
    await ws.recv()  # joined ack
    return ws


async def _send(ws, room_id: str, text: str) -> dict:
    await ws.send(json.dumps({"type": "send", "room_id": room_id, "text": text}))
    # Skip past any live-fanout "message" frames (our own echo, or other
    # room members' messages) that may already be queued ahead of our own
    # ack — this same loop naturally drains them, no separate step needed.
    while True:
        ack = json.loads(await ws.recv())
        if ack["type"] in ("sent", "error"):
            return ack


async def test_send_is_throttled_after_the_limit_and_recovers_after_the_window():
    user_id = f"rate-limit-{uuid.uuid4().hex[:8]}"
    room_id = f"rate-limit-room-{uuid.uuid4().hex[:8]}"
    ws = await _connect_and_join(user_id, room_id)

    for i in range(MAX_MESSAGES):
        ack = await _send(ws, room_id, f"msg-{i}")
        assert ack["type"] == "sent", f"message {i} unexpectedly throttled: {ack}"

    ack = await _send(ws, room_id, "one too many")
    assert ack["type"] == "error"
    assert ack["error"] == "rate_limited"
    assert ack["retry_after_seconds"] > 0

    # a second rejected send shouldn't extend the window
    ack = await _send(ws, room_id, "still too many")
    assert ack["type"] == "error"

    await asyncio.sleep(WINDOW_SECONDS + 1)

    ack = await _send(ws, room_id, "after the window")
    assert ack["type"] == "sent"

    await ws.close()


async def test_rate_limit_is_scoped_per_user_not_per_connection():
    room_id = f"rate-limit-scope-{uuid.uuid4().hex[:8]}"
    alice = await _connect_and_join(f"alice-{uuid.uuid4().hex[:8]}", room_id)
    bob = await _connect_and_join(f"bob-{uuid.uuid4().hex[:8]}", room_id)

    for i in range(MAX_MESSAGES):
        ack = await _send(alice, room_id, f"msg-{i}")
        assert ack["type"] == "sent"

    ack = await _send(alice, room_id, "throttled")
    assert ack["type"] == "error"

    # bob has never sent anything — his own limit is untouched
    ack = await _send(bob, room_id, "hi from bob")
    assert ack["type"] == "sent"

    await asyncio.gather(alice.close(), bob.close())
