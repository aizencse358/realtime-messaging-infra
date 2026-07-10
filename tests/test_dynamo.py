import pytest

from src import dynamo

pytestmark = pytest.mark.usefixtures("dynamo_tables")


async def test_put_message_persists_with_composite_sort_key():
    stored = await dynamo.put_message("room1", "alice", "hello")

    assert stored["conversation_id"] == "room1"
    assert stored["sender_id"] == "alice"
    assert stored["text"] == "hello"
    assert stored["sort_key"] == f"{stored['timestamp_ms']}#{stored['message_id']}"


async def test_update_last_seen_writes_attribute():
    await dynamo.update_last_seen("alice", 12345)

    table = dynamo._users_table()
    item = table.get_item(Key={"user_id": "alice"})["Item"]
    assert item["last_seen_at"] == 12345


async def test_room_member_put_and_delete():
    await dynamo.put_room_member("room1", "alice")

    table = dynamo._room_members_table()
    item = table.get_item(Key={"room_id": "room1", "user_id": "alice"})["Item"]
    assert item["user_id"] == "alice"

    await dynamo.delete_room_member("room1", "alice")
    assert "Item" not in table.get_item(Key={"room_id": "room1", "user_id": "alice"})


async def test_room_members_gsi_reverses_lookup_by_user():
    await dynamo.put_room_member("room1", "alice")
    await dynamo.put_room_member("room2", "alice")

    table = dynamo._room_members_table()
    result = table.query(
        IndexName=dynamo.USER_ROOMS_GSI,
        KeyConditionExpression="user_id = :u",
        ExpressionAttributeValues={":u": "alice"},
    )
    room_ids = {item["room_id"] for item in result["Items"]}
    assert room_ids == {"room1", "room2"}


def _seed_message(room_id: str, ts_ms: int, message_id: str, text: str) -> None:
    dynamo._messages_table().put_item(
        Item={
            "conversation_id": room_id,
            "sort_key": f"{ts_ms}#{message_id}",
            "message_id": message_id,
            "sender_id": "alice",
            "text": text,
            "timestamp_ms": ts_ms,
        }
    )


async def test_get_room_messages_returns_oldest_to_newest():
    _seed_message("room1", 100, "m1", "first")
    _seed_message("room1", 200, "m2", "second")
    _seed_message("room1", 300, "m3", "third")

    items = await dynamo.get_room_messages("room1", limit=50)

    assert [item["text"] for item in items] == ["third", "second", "first"]


async def test_get_room_messages_paginates_with_before_cursor():
    _seed_message("room1", 100, "m1", "first")
    _seed_message("room1", 200, "m2", "second")
    _seed_message("room1", 300, "m3", "third")

    first_page = await dynamo.get_room_messages("room1", limit=2)
    assert [item["text"] for item in first_page] == ["third", "second"]

    cursor = first_page[-1]["sort_key"]
    second_page = await dynamo.get_room_messages("room1", limit=2, before=cursor)
    assert [item["text"] for item in second_page] == ["first"]


async def test_get_room_messages_scoped_to_room():
    _seed_message("room1", 100, "m1", "in room1")
    _seed_message("room2", 100, "m2", "in room2")

    items = await dynamo.get_room_messages("room1", limit=50)

    assert [item["text"] for item in items] == ["in room1"]
