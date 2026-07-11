import asyncio
import json
import uuid

import httpx
import pytest
import websockets

from tests.integration.conftest import http_url, ws_url

pytestmark = pytest.mark.integration


async def _connect(user_id: str):
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    connected = json.loads(await ws.recv())
    assert connected["type"] == "connected"
    return ws, connected["gateway_id"]


async def test_join_send_receive_round_trip():
    room_id = f"e2e-{uuid.uuid4().hex[:8]}"
    alice, _ = await _connect(f"alice-{uuid.uuid4().hex[:8]}")
    bob, _ = await _connect(f"bob-{uuid.uuid4().hex[:8]}")

    await alice.send(json.dumps({"type": "join", "room_id": room_id}))
    assert json.loads(await alice.recv())["type"] == "joined"
    await bob.send(json.dumps({"type": "join", "room_id": room_id}))
    assert json.loads(await bob.recv())["type"] == "joined"

    await alice.send(json.dumps({"type": "send", "room_id": room_id, "text": "hi bob"}))
    sent_ack = json.loads(await alice.recv())
    assert sent_ack["type"] == "sent"

    echo = json.loads(await alice.recv())
    assert echo["type"] == "message"
    assert echo["text"] == "hi bob"

    delivered = json.loads(await bob.recv())
    assert delivered["message_id"] == echo["message_id"]
    assert delivered["text"] == "hi bob"

    await alice.close()
    await bob.close()


async def test_message_history_persists_and_paginates():
    room_id = f"history-{uuid.uuid4().hex[:8]}"
    ws, _ = await _connect(f"carol-{uuid.uuid4().hex[:8]}")

    await ws.send(json.dumps({"type": "join", "room_id": room_id}))
    await ws.recv()

    for i in range(3):
        await ws.send(json.dumps({"type": "send", "room_id": room_id, "text": f"msg-{i}"}))
        await ws.recv()  # sent ack
        await ws.recv()  # echo
    await ws.close()

    async with httpx.AsyncClient() as client:
        resp = await client.get(http_url(f"/rooms/{room_id}/messages"), params={"limit": 2})
        assert resp.status_code == 200
        page1 = resp.json()
        assert [m["text"] for m in page1["messages"]] == ["msg-1", "msg-2"]
        assert page1["next_before"] is not None

        resp = await client.get(
            http_url(f"/rooms/{room_id}/messages"),
            params={"limit": 2, "before": page1["next_before"]},
        )
        page2 = resp.json()
        assert [m["text"] for m in page2["messages"]] == ["msg-0"]
        assert page2["next_before"] is None


async def test_presence_and_registry_reflect_live_connection():
    user_id = f"dana-{uuid.uuid4().hex[:8]}"
    ws, gateway_id = await _connect(user_id)

    async with httpx.AsyncClient() as client:
        presence = (await client.get(http_url(f"/presence/{user_id}"))).json()
        assert presence["online"] is True

        registry = (await client.get(http_url(f"/registry/{user_id}"))).json()
        assert registry["gateway_id"] == gateway_id

    await ws.close()

    # give the gateway's disconnect handler a moment to clear the keys
    for _ in range(25):
        async with httpx.AsyncClient() as client:
            presence = (await client.get(http_url(f"/presence/{user_id}"))).json()
        if presence["online"] is False:
            break
        await asyncio.sleep(0.2)

    assert presence["online"] is False
