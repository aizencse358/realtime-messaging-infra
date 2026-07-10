import asyncio

import pytest

from src.config import presence_key, registry_key
from src.connection_manager import ConnectionManager

pytestmark = pytest.mark.usefixtures("dynamo_tables")


class FakeWebSocket:
    def __init__(self):
        self.accepted = False
        self.sent: list[str] = []

    async def accept(self):
        self.accepted = True

    async def send_text(self, text: str):
        self.sent.append(text)


async def _wait_for_delivery(sockets: list[FakeWebSocket], count: int = 1, attempts: int = 50):
    for _ in range(attempts):
        if all(len(ws.sent) >= count for ws in sockets):
            return
        await asyncio.sleep(0.02)


@pytest.fixture
async def manager(fake_redis):
    mgr = ConnectionManager()
    await mgr.start()
    yield mgr
    await mgr.stop()


async def test_connect_accepts_and_sets_presence_and_registry(manager, fake_redis):
    ws = FakeWebSocket()
    await manager.connect("alice", ws)

    assert ws.accepted
    assert await fake_redis.get(presence_key("alice")) == "online"
    assert await fake_redis.get(registry_key("alice")) is not None
    assert await fake_redis.ttl(presence_key("alice")) > 0


async def test_join_room_refcounts_subscription_across_local_members(manager):
    alice_ws, bob_ws = FakeWebSocket(), FakeWebSocket()
    await manager.connect("alice", alice_ws)
    await manager.connect("bob", bob_ws)

    await manager.join_room("general", "alice")
    assert manager.room_members["general"] == {"alice"}

    await manager.join_room("general", "bob")
    assert manager.room_members["general"] == {"alice", "bob"}

    await manager.leave_room("general", "alice")
    assert manager.room_members["general"] == {"bob"}

    await manager.leave_room("general", "bob")
    assert "general" not in manager.room_members


async def test_send_fans_out_to_all_local_room_members(manager):
    alice_ws, bob_ws = FakeWebSocket(), FakeWebSocket()
    await manager.connect("alice", alice_ws)
    await manager.connect("bob", bob_ws)
    await manager.join_room("general", "alice")
    await manager.join_room("general", "bob")

    await manager.publish_to_room("general", {"type": "message", "text": "hello"})

    await _wait_for_delivery([alice_ws, bob_ws])
    assert len(alice_ws.sent) == 1
    assert len(bob_ws.sent) == 1
    assert "hello" in alice_ws.sent[0]


async def test_direct_message_delivers_only_to_target_user(manager):
    alice_ws, bob_ws = FakeWebSocket(), FakeWebSocket()
    await manager.connect("alice", alice_ws)
    await manager.connect("bob", bob_ws)

    await manager.publish_to_user("bob", {"type": "message", "text": "psst"})

    await _wait_for_delivery([bob_ws])
    assert len(bob_ws.sent) == 1
    assert len(alice_ws.sent) == 0


async def test_disconnect_flushes_last_seen_and_clears_presence(manager, fake_redis):
    from src import dynamo

    ws = FakeWebSocket()
    await manager.connect("alice", ws)
    await manager.join_room("general", "alice")

    await manager.disconnect("alice")

    assert await fake_redis.get(presence_key("alice")) is None
    assert await fake_redis.get(registry_key("alice")) is None
    assert "general" not in manager.room_members

    item = dynamo._users_table().get_item(Key={"user_id": "alice"})["Item"]
    assert "last_seen_at" in item
