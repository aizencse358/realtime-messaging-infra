import asyncio
import time
import uuid
from functools import lru_cache

import boto3
from botocore.exceptions import ClientError

from src.config import (
    AWS_ACCESS_KEY_ID,
    AWS_REGION,
    AWS_SECRET_ACCESS_KEY,
    DYNAMODB_ENDPOINT_URL,
    MESSAGES_TABLE,
    ROOM_MEMBERS_TABLE,
    USERS_TABLE,
)

USER_ROOMS_GSI = "gsi_user_rooms"


@lru_cache
def _resource():
    return boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )


def _create_table_if_missing(**kwargs) -> None:
    ddb = _resource()
    try:
        ddb.create_table(**kwargs)
        ddb.meta.client.get_waiter("table_exists").wait(TableName=kwargs["TableName"])
    except ClientError as exc:
        if exc.response["Error"]["Code"] != "ResourceInUseException":
            raise


def init_tables_sync() -> None:
    """Idempotently create the Messages, Users, and RoomMembers tables."""
    _create_table_if_missing(
        TableName=MESSAGES_TABLE,
        KeySchema=[
            {"AttributeName": "conversation_id", "KeyType": "HASH"},
            {"AttributeName": "sort_key", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "conversation_id", "AttributeType": "S"},
            {"AttributeName": "sort_key", "AttributeType": "S"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )

    _create_table_if_missing(
        TableName=USERS_TABLE,
        KeySchema=[{"AttributeName": "user_id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "user_id", "AttributeType": "S"}],
        BillingMode="PAY_PER_REQUEST",
    )

    _create_table_if_missing(
        TableName=ROOM_MEMBERS_TABLE,
        KeySchema=[
            {"AttributeName": "room_id", "KeyType": "HASH"},
            {"AttributeName": "user_id", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "room_id", "AttributeType": "S"},
            {"AttributeName": "user_id", "AttributeType": "S"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": USER_ROOMS_GSI,
                # Reversed key order: lets us query "which rooms is user X in"
                # without a table scan.
                "KeySchema": [
                    {"AttributeName": "user_id", "KeyType": "HASH"},
                    {"AttributeName": "room_id", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        BillingMode="PAY_PER_REQUEST",
    )


async def init_tables() -> None:
    await asyncio.to_thread(init_tables_sync)


def _messages_table():
    return _resource().Table(MESSAGES_TABLE)


def _users_table():
    return _resource().Table(USERS_TABLE)


def _room_members_table():
    return _resource().Table(ROOM_MEMBERS_TABLE)


def _put_message_sync(room_id: str, sender_id: str, text: str) -> dict:
    ts_ms = int(time.time() * 1000)
    message_id = uuid.uuid4().hex
    item = {
        "conversation_id": room_id,
        "sort_key": f"{ts_ms}#{message_id}",
        "message_id": message_id,
        "sender_id": sender_id,
        "text": text,
        "timestamp_ms": ts_ms,
    }
    _messages_table().put_item(Item=item)
    return item


async def put_message(room_id: str, sender_id: str, text: str) -> dict:
    return await asyncio.to_thread(_put_message_sync, room_id, sender_id, text)


def _update_last_seen_sync(user_id: str, last_seen_at: int) -> None:
    _users_table().update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET last_seen_at = :ts",
        ExpressionAttributeValues={":ts": last_seen_at},
    )


async def update_last_seen(user_id: str, last_seen_at: int | None = None) -> None:
    last_seen_at = last_seen_at if last_seen_at is not None else int(time.time())
    await asyncio.to_thread(_update_last_seen_sync, user_id, last_seen_at)


def _put_room_member_sync(room_id: str, user_id: str) -> None:
    _room_members_table().put_item(
        Item={"room_id": room_id, "user_id": user_id, "joined_at": int(time.time())}
    )


async def put_room_member(room_id: str, user_id: str) -> None:
    await asyncio.to_thread(_put_room_member_sync, room_id, user_id)


def _delete_room_member_sync(room_id: str, user_id: str) -> None:
    _room_members_table().delete_item(Key={"room_id": room_id, "user_id": user_id})


async def delete_room_member(room_id: str, user_id: str) -> None:
    await asyncio.to_thread(_delete_room_member_sync, room_id, user_id)
