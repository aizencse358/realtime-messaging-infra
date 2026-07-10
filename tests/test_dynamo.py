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
