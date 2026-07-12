import asyncio
import functools
import logging
import time
import uuid
from functools import lru_cache

import boto3
from boto3.dynamodb.conditions import Key
from botocore.config import Config
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

logger = logging.getLogger("gateway.dynamo")

USER_ROOMS_GSI = "gsi_user_rooms"

# boto3's defaults are tuned for AWS's real network, not "the container
# next to me is unreachable" — left alone, a DynamoDB Local outage makes
# calls hang for a long time (connect timeout + several retries) before
# ever raising, which reads as a stalled connection rather than a fast,
# clean failure. Bound both.
_BOTO_CONFIG = Config(
    connect_timeout=2,
    read_timeout=3,
    retries={"max_attempts": 2, "mode": "standard"},
)


@lru_cache
def _resource():
    return boto3.resource(
        "dynamodb",
        endpoint_url=DYNAMODB_ENDPOINT_URL,
        region_name=AWS_REGION,
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        config=_BOTO_CONFIG,
    )


def _is_missing_table(exc: ClientError) -> bool:
    return exc.response["Error"]["Code"] == "ResourceNotFoundException"


def _resilient(fn):
    """DynamoDB Local run with `-inMemory` loses its schema on restart —
    without a persistent volume that's every operation's problem, and even
    with one, someone can still wipe the container's volume. Rather than
    require a gateway restart to recover, recreate the tables (idempotent)
    and retry once whenever an operation discovers they're gone."""

    @functools.wraps(fn)
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ClientError as exc:
            if not _is_missing_table(exc):
                raise
            logger.warning("event=dynamo_tables_missing action=recreate_and_retry")
            init_tables_sync()
            return fn(*args, **kwargs)

    return wrapper


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


@_resilient
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


@_resilient
def _get_room_messages_sync(room_id: str, limit: int, before: str | None) -> list[dict]:
    key_condition = Key("conversation_id").eq(room_id)
    if before is not None:
        key_condition &= Key("sort_key").lt(before)

    response = _messages_table().query(
        KeyConditionExpression=key_condition,
        # Newest first, so a page is always "the most recent `limit`
        # messages before `before`" — the natural shape for chat history
        # (load latest, then page backward for older messages).
        ScanIndexForward=False,
        Limit=limit,
    )
    return response.get("Items", [])


async def get_room_messages(
    room_id: str, limit: int = 50, before: str | None = None
) -> list[dict]:
    return await asyncio.to_thread(_get_room_messages_sync, room_id, limit, before)


@_resilient
def _update_last_seen_sync(user_id: str, last_seen_at: int) -> None:
    _users_table().update_item(
        Key={"user_id": user_id},
        UpdateExpression="SET last_seen_at = :ts",
        ExpressionAttributeValues={":ts": last_seen_at},
    )


async def update_last_seen(user_id: str, last_seen_at: int | None = None) -> None:
    last_seen_at = last_seen_at if last_seen_at is not None else int(time.time())
    await asyncio.to_thread(_update_last_seen_sync, user_id, last_seen_at)


@_resilient
def _put_room_member_sync(room_id: str, user_id: str, initial_last_read_sort_key: str) -> None:
    # if_not_exists so this is safe to call on every join, not just the
    # first: a returning member's joined_at and (more importantly)
    # last_read_sort_key must survive a rejoin, or "unread since I was last
    # here" would reset to zero every time.
    _room_members_table().update_item(
        Key={"room_id": room_id, "user_id": user_id},
        UpdateExpression=(
            "SET joined_at = if_not_exists(joined_at, :now), "
            "last_read_sort_key = if_not_exists(last_read_sort_key, :initial)"
        ),
        ExpressionAttributeValues={":now": int(time.time()), ":initial": initial_last_read_sort_key},
    )


async def put_room_member(
    room_id: str, user_id: str, initial_last_read_sort_key: str = ""
) -> None:
    await asyncio.to_thread(
        _put_room_member_sync, room_id, user_id, initial_last_read_sort_key
    )


@_resilient
def _delete_room_member_sync(room_id: str, user_id: str) -> None:
    _room_members_table().delete_item(Key={"room_id": room_id, "user_id": user_id})


async def delete_room_member(room_id: str, user_id: str) -> None:
    await asyncio.to_thread(_delete_room_member_sync, room_id, user_id)


@_resilient
def _get_room_member_sync(room_id: str, user_id: str) -> dict | None:
    resp = _room_members_table().get_item(Key={"room_id": room_id, "user_id": user_id})
    return resp.get("Item")


async def get_room_member(room_id: str, user_id: str) -> dict | None:
    return await asyncio.to_thread(_get_room_member_sync, room_id, user_id)


@_resilient
def _mark_room_read_sync(room_id: str, user_id: str, sort_key: str) -> None:
    _room_members_table().update_item(
        Key={"room_id": room_id, "user_id": user_id},
        UpdateExpression="SET last_read_sort_key = :sk",
        ExpressionAttributeValues={":sk": sort_key},
    )


async def mark_room_read(room_id: str, user_id: str, sort_key: str) -> None:
    await asyncio.to_thread(_mark_room_read_sync, room_id, user_id, sort_key)


@_resilient
def _count_unread_messages_sync(room_id: str, last_read_sort_key: str) -> int:
    key_condition = Key("conversation_id").eq(room_id)
    if last_read_sort_key:
        key_condition &= Key("sort_key").gt(last_read_sort_key)

    total = 0
    kwargs: dict = {"KeyConditionExpression": key_condition, "Select": "COUNT"}
    while True:
        resp = _messages_table().query(**kwargs)
        total += resp["Count"]
        if "LastEvaluatedKey" not in resp:
            return total
        kwargs["ExclusiveStartKey"] = resp["LastEvaluatedKey"]


async def count_unread_messages(room_id: str, last_read_sort_key: str) -> int:
    return await asyncio.to_thread(_count_unread_messages_sync, room_id, last_read_sort_key)


def _latest_message_sort_key_sync(room_id: str) -> str:
    items = _get_room_messages_sync(room_id, limit=1, before=None)
    return items[0]["sort_key"] if items else ""


async def latest_message_sort_key(room_id: str) -> str:
    return await asyncio.to_thread(_latest_message_sort_key_sync, room_id)
