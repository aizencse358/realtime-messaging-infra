import asyncio
import json
import logging
import time

from fastapi import WebSocket
from redis.asyncio.client import PubSub
from redis.exceptions import RedisError

from src import dynamo
from src.config import (
    GATEWAY_ID,
    PRESENCE_HEARTBEAT_SECONDS,
    PRESENCE_TTL_SECONDS,
    REGISTRY_TTL_SECONDS,
    presence_key,
    registry_key,
    room_channel,
    user_channel,
)
from src.metrics import (
    messages_delivered_total,
    room_joins_total,
    room_leaves_total,
    room_subscribes_total,
    room_subscriptions_active,
    room_unsubscribes_total,
    ws_connections_active,
)
from src.redis_client import get_redis

logger = logging.getLogger("gateway.connections")


class ConnectionManager:
    """Tracks this gateway instance's local WebSocket connections and room
    membership, and owns the single Redis pub/sub connection used to fan
    in/out messages for the rooms/users this instance actually serves.

    Room channel subscriptions are refcounted by local membership: the first
    local member to join a room triggers a Redis SUBSCRIBE, and the last
    local member to leave triggers an UNSUBSCRIBE. This keeps each instance's
    pub/sub connection scoped to only the traffic it needs, however many
    total rooms exist cluster-wide.
    """

    def __init__(self) -> None:
        self.connections: dict[str, WebSocket] = {}
        self.room_members: dict[str, set[str]] = {}
        self.user_rooms: dict[str, set[str]] = {}
        self.heartbeat_tasks: dict[str, asyncio.Task] = {}
        self._lock = asyncio.Lock()
        self._pubsub: PubSub | None = None
        self._listener_task: asyncio.Task | None = None

    async def start(self) -> None:
        redis = get_redis()
        self._pubsub = redis.pubsub()
        self._listener_task = asyncio.create_task(self._listen())

        def _log_crash(task: asyncio.Task) -> None:
            if task.cancelled():
                return
            exc = task.exception()
            if exc is not None:
                logger.error("pubsub listener task crashed", exc_info=exc)

        self._listener_task.add_done_callback(_log_crash)
        logger.info("pubsub listener task started")

    async def stop(self) -> None:
        if self._listener_task:
            self._listener_task.cancel()
        if self._pubsub:
            await self._pubsub.aclose()

    # ---- connection lifecycle ----

    async def connect(self, user_id: str, websocket: WebSocket) -> None:
        await websocket.accept()
        async with self._lock:
            self.connections[user_id] = websocket
            self.user_rooms.setdefault(user_id, set())
            await self._subscribe_best_effort(user_channel(user_id))
        await self._touch_presence_and_registry(user_id)
        self.heartbeat_tasks[user_id] = asyncio.create_task(
            self._heartbeat_loop(user_id)
        )
        ws_connections_active.labels(GATEWAY_ID).inc()
        logger.info("event=ws_connected user_id=%s gateway_id=%s", user_id, GATEWAY_ID)

    async def disconnect(self, user_id: str) -> None:
        task = self.heartbeat_tasks.pop(user_id, None)
        if task:
            task.cancel()

        async with self._lock:
            self.connections.pop(user_id, None)
            rooms = self.user_rooms.pop(user_id, set())
            await self._unsubscribe_best_effort(user_channel(user_id))
            for room_id in list(rooms):
                await self._leave_room_locked(room_id, user_id)

        try:
            redis = get_redis()
            await redis.delete(presence_key(user_id), registry_key(user_id))
        except RedisError:
            logger.warning(
                "event=redis_unavailable action=clear_presence_registry user_id=%s", user_id
            )
        last_seen_at = int(time.time())
        await dynamo.update_last_seen(user_id, last_seen_at)
        ws_connections_active.labels(GATEWAY_ID).dec()
        logger.info(
            "event=ws_disconnected user_id=%s gateway_id=%s last_seen_at=%s",
            user_id,
            GATEWAY_ID,
            last_seen_at,
        )

    # ---- room membership (refcounted subscribe/unsubscribe) ----

    async def join_room(self, room_id: str, user_id: str) -> None:
        async with self._lock:
            members = self.room_members.setdefault(room_id, set())
            is_first_local_member = len(members) == 0
            members.add(user_id)
            self.user_rooms[user_id].add(room_id)
            if is_first_local_member:
                await self._subscribe_best_effort(room_channel(room_id))
                room_subscribes_total.labels(GATEWAY_ID).inc()
                room_subscriptions_active.labels(GATEWAY_ID).inc()
                logger.info(
                    "event=room_subscribed room_id=%s gateway_id=%s reason=first_local_member",
                    room_id,
                    GATEWAY_ID,
                )
        await dynamo.put_room_member(room_id, user_id)
        room_joins_total.labels(GATEWAY_ID).inc()
        logger.info("event=room_joined room_id=%s user_id=%s", room_id, user_id)

    async def leave_room(self, room_id: str, user_id: str) -> None:
        async with self._lock:
            await self._leave_room_locked(room_id, user_id)
        await dynamo.delete_room_member(room_id, user_id)
        room_leaves_total.labels(GATEWAY_ID).inc()
        logger.info("event=room_left room_id=%s user_id=%s", room_id, user_id)

    async def _leave_room_locked(self, room_id: str, user_id: str) -> None:
        members = self.room_members.get(room_id)
        if not members or user_id not in members:
            return
        members.discard(user_id)
        self.user_rooms.get(user_id, set()).discard(room_id)
        if not members:
            del self.room_members[room_id]
            await self._unsubscribe_best_effort(room_channel(room_id))
            room_unsubscribes_total.labels(GATEWAY_ID).inc()
            room_subscriptions_active.labels(GATEWAY_ID).dec()
            logger.info(
                "event=room_unsubscribed room_id=%s gateway_id=%s reason=last_local_member_left",
                room_id,
                GATEWAY_ID,
            )

    # ---- subscribe/unsubscribe, tolerant of a Redis outage ----
    #
    # Local bookkeeping (room_members / user_rooms / connections) is always
    # updated first regardless of whether the Redis call below succeeds —
    # it's the source of truth _listen() uses to resubscribe everything
    # once connectivity comes back, so a subscribe/unsubscribe that fails
    # here isn't lost, just deferred.

    async def _subscribe_best_effort(self, channel: str) -> None:
        try:
            await self._pubsub.subscribe(channel)
        except RedisError:
            logger.warning("event=redis_unavailable action=subscribe channel=%s", channel)

    async def _unsubscribe_best_effort(self, channel: str) -> None:
        try:
            await self._pubsub.unsubscribe(channel)
        except RedisError:
            logger.warning("event=redis_unavailable action=unsubscribe channel=%s", channel)

    async def _resubscribe_all(self) -> None:
        channels = [user_channel(uid) for uid in self.connections] + [
            room_channel(rid) for rid in self.room_members
        ]
        if not channels:
            return
        await self._pubsub.subscribe(*channels)
        logger.info("event=redis_resubscribed gateway_id=%s channel_count=%d", GATEWAY_ID, len(channels))

    # ---- presence + registry ----

    async def _touch_presence_and_registry(self, user_id: str) -> None:
        redis = get_redis()
        async with redis.pipeline(transaction=False) as pipe:
            pipe.set(presence_key(user_id), "online", ex=PRESENCE_TTL_SECONDS)
            pipe.set(registry_key(user_id), GATEWAY_ID, ex=REGISTRY_TTL_SECONDS)
            await pipe.execute()

    async def _heartbeat_loop(self, user_id: str) -> None:
        try:
            while True:
                await asyncio.sleep(PRESENCE_HEARTBEAT_SECONDS)
                try:
                    await self._touch_presence_and_registry(user_id)
                except RedisError:
                    logger.warning(
                        "event=redis_unavailable action=heartbeat user_id=%s", user_id
                    )
        except asyncio.CancelledError:
            pass

    # ---- publish + fan-in ----

    async def publish_to_room(self, room_id: str, payload: dict) -> None:
        redis = get_redis()
        await redis.publish(room_channel(room_id), json.dumps(payload))

    async def publish_to_user(self, user_id: str, payload: dict) -> None:
        redis = get_redis()
        await redis.publish(user_channel(user_id), json.dumps(payload))

    async def _listen(self) -> None:
        # redis-py's async PubSub is a single connection shared with
        # subscribe()/unsubscribe() calls made from other coroutines (room
        # join/leave, connect/disconnect). Reading and (un)subscribing
        # concurrently on that connection corrupts the protocol stream, so
        # every access to self._pubsub is serialized through self._lock —
        # short-poll get_message() rather than the blocking listen()
        # iterator so the lock is held only briefly per loop iteration.
        assert self._pubsub is not None
        while True:
            try:
                async with self._lock:
                    # redis-py raises instead of returning None if
                    # get_message() is called before any
                    # subscribe()/psubscribe() has ever happened on this
                    # connection (e.g. at gateway startup, before the first
                    # client connects or joins a room).
                    if self._pubsub.connection is None:
                        message = None
                    else:
                        message = await self._pubsub.get_message(
                            ignore_subscribe_messages=True, timeout=0.01
                        )
            except RedisError:
                await self._reconnect_pubsub()
                continue

            if message is None:
                await asyncio.sleep(0.01)
                continue
            if message["type"] != "message":
                continue

            channel: str = message["channel"]
            try:
                payload = json.loads(message["data"])
            except (TypeError, ValueError):
                continue

            if channel.startswith("chat:room:"):
                room_id = channel.removeprefix("chat:room:")
                await self._fanout_to_room(room_id, payload)
            elif channel.startswith("chat:user:"):
                user_id = channel.removeprefix("chat:user:")
                await self._deliver_to_user(user_id, payload)

    async def _reconnect_pubsub(self) -> None:
        """The dedicated pub/sub connection doesn't reconnect itself the way
        redis-py's regular connection pool does — a Redis restart otherwise
        permanently kills fanout on this gateway until the process restarts.
        Recreate it and resubscribe to everything currently tracked locally
        (room_members / connections are the source of truth, independent of
        whatever Redis thinks is subscribed), retrying with backoff for as
        long as Redis stays unreachable.
        """
        logger.warning("event=redis_unavailable action=pubsub_listener gateway_id=%s", GATEWAY_ID)
        async with self._lock:
            try:
                await self._pubsub.aclose()
            except RedisError:
                pass
            self._pubsub = get_redis().pubsub()

        backoff = 0.5
        while True:
            try:
                async with self._lock:
                    await self._resubscribe_all()
                return
            except RedisError:
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 5.0)

    async def _fanout_to_room(self, room_id: str, payload: dict) -> None:
        members = self.room_members.get(room_id, set())
        text = json.dumps(payload)
        for user_id in list(members):
            ws = self.connections.get(user_id)
            if ws is not None:
                try:
                    await ws.send_text(text)
                    messages_delivered_total.labels(GATEWAY_ID).inc()
                except Exception:
                    logger.exception("failed to deliver to %s in room %s", user_id, room_id)

    async def _deliver_to_user(self, user_id: str, payload: dict) -> None:
        ws = self.connections.get(user_id)
        if ws is not None:
            try:
                await ws.send_text(json.dumps(payload))
            except Exception:
                logger.exception("failed to deliver direct message to %s", user_id)


manager = ConnectionManager()
