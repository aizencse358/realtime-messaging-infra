import json
import logging
import time
from contextlib import asynccontextmanager

from botocore.exceptions import BotoCoreError, ClientError
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
from redis.exceptions import RedisError

from src import dynamo
from src.config import GATEWAY_ID, presence_key, registry_key
from src.connection_manager import manager
from src.metrics import message_persist_seconds, messages_sent_total, rate_limit_exceeded_total
from src.observability import configure_logging, install_request_logging
from src.rate_limit import check_send_rate_limit
from src.redis_client import close_redis, get_redis

configure_logging()
logger = logging.getLogger("gateway")


@asynccontextmanager
async def lifespan(app: FastAPI):
    await dynamo.init_tables()
    await manager.start()
    logger.info("event=gateway_started gateway_id=%s", GATEWAY_ID)
    yield
    await manager.stop()
    await close_redis()


app = FastAPI(title="realtime-messaging-gateway", lifespan=lifespan)
install_request_logging(app)


@app.get("/healthz")
async def healthz():
    return {"status": "ok", "gateway_id": GATEWAY_ID}


@app.get("/presence/{user_id}")
async def get_presence(user_id: str):
    redis = get_redis()
    online = await redis.get(presence_key(user_id))
    return {"user_id": user_id, "online": online is not None}


@app.get("/registry/{user_id}")
async def get_registry(user_id: str):
    redis = get_redis()
    owner = await redis.get(registry_key(user_id))
    return {"user_id": user_id, "gateway_id": owner}


@app.get("/rooms/{room_id}/messages")
async def get_room_messages(room_id: str, limit: int = 50, before: str | None = None):
    limit = max(1, min(limit, 200))
    items = await dynamo.get_room_messages(room_id, limit=limit, before=before)

    messages = [
        {
            "message_id": item["message_id"],
            "sender_id": item["sender_id"],
            "text": item["text"],
            "timestamp_ms": int(item["timestamp_ms"]),
            "sort_key": item["sort_key"],
        }
        for item in reversed(items)  # oldest-to-newest for display
    ]
    next_before = items[-1]["sort_key"] if len(items) == limit else None

    return {"room_id": room_id, "messages": messages, "next_before": next_before}


@app.get("/rooms/{room_id}/unread/{user_id}")
async def get_unread_count(room_id: str, user_id: str):
    member = await dynamo.get_room_member(room_id, user_id)
    if member is None:
        return {"room_id": room_id, "user_id": user_id, "unread_count": 0, "last_read_sort_key": None}

    last_read_sort_key = member.get("last_read_sort_key", "")
    unread_count = await dynamo.count_unread_messages(room_id, last_read_sort_key)
    return {
        "room_id": room_id,
        "user_id": user_id,
        "unread_count": unread_count,
        "last_read_sort_key": last_read_sort_key or None,
    }


@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


async def _handle_frame(user_id: str, websocket: WebSocket, raw: str) -> None:
    try:
        frame = json.loads(raw)
    except ValueError:
        await websocket.send_text(json.dumps({"type": "error", "error": "invalid_json"}))
        return

    try:
        await _dispatch_frame(user_id, websocket, frame)
    except RedisError:
        # Redis being transiently unreachable (e.g. a restart) shouldn't
        # tear down an otherwise-healthy WebSocket connection — surface it
        # as a frame-level error so the client can retry once Redis is back,
        # rather than losing the whole session over one blip.
        logger.warning(
            "event=redis_unavailable action=handle_frame user_id=%s frame_type=%s",
            user_id,
            frame.get("type"),
        )
        await websocket.send_text(json.dumps({"type": "error", "error": "redis_unavailable"}))
    except (BotoCoreError, ClientError):
        # Same idea for DynamoDB: a transient outage (or the moment between
        # discovering a table is missing and dynamo._resilient recreating
        # it) shouldn't cost the client its connection either.
        logger.warning(
            "event=dynamo_unavailable action=handle_frame user_id=%s frame_type=%s",
            user_id,
            frame.get("type"),
        )
        await websocket.send_text(json.dumps({"type": "error", "error": "dynamo_unavailable"}))


async def _dispatch_frame(user_id: str, websocket: WebSocket, frame: dict) -> None:
    msg_type = frame.get("type")

    if msg_type == "join":
        room_id = frame["room_id"]
        unread_count = await manager.join_room(room_id, user_id)
        await websocket.send_text(
            json.dumps({"type": "joined", "room_id": room_id, "unread_count": unread_count})
        )

    elif msg_type == "leave":
        room_id = frame["room_id"]
        await manager.leave_room(room_id, user_id)
        await websocket.send_text(json.dumps({"type": "left", "room_id": room_id}))

    elif msg_type == "mark_read":
        room_id = frame["room_id"]
        sort_key = frame["sort_key"]
        await dynamo.mark_room_read(room_id, user_id, sort_key)
        await websocket.send_text(
            json.dumps({"type": "read_ack", "room_id": room_id, "sort_key": sort_key})
        )

    elif msg_type == "send":
        room_id = frame["room_id"]
        text = frame["text"]
        client_msg_id = frame.get("client_msg_id")
        sent_at_ms = frame.get("sent_at_ms")

        rate_limit = await check_send_rate_limit(user_id)
        if not rate_limit.allowed:
            rate_limit_exceeded_total.labels(GATEWAY_ID).inc()
            logger.info(
                "event=rate_limited sender_id=%s room_id=%s retry_after_seconds=%d",
                user_id,
                room_id,
                rate_limit.retry_after_seconds,
            )
            await websocket.send_text(
                json.dumps(
                    {
                        "type": "error",
                        "error": "rate_limited",
                        "retry_after_seconds": rate_limit.retry_after_seconds,
                    }
                )
            )
            return

        t0 = time.perf_counter()
        stored = await dynamo.put_message(room_id, user_id, text)
        duration_seconds = time.perf_counter() - t0
        duration_ms = duration_seconds * 1000
        message_persist_seconds.labels(GATEWAY_ID).observe(duration_seconds)
        messages_sent_total.labels(GATEWAY_ID).inc()
        logger.info(
            "event=message_sent room_id=%s sender_id=%s message_id=%s duration_ms=%.2f",
            room_id,
            user_id,
            stored["message_id"],
            duration_ms,
        )
        payload = {
            "type": "message",
            "room_id": room_id,
            "sender_id": user_id,
            "text": text,
            "message_id": stored["message_id"],
            "timestamp_ms": stored["timestamp_ms"],
            "sort_key": stored["sort_key"],
            "client_msg_id": client_msg_id,
            "sent_at_ms": sent_at_ms,
        }
        await manager.publish_to_room(room_id, payload)
        await websocket.send_text(
            json.dumps(
                {
                    "type": "sent",
                    "room_id": room_id,
                    "message_id": stored["message_id"],
                    "client_msg_id": client_msg_id,
                }
            )
        )

    elif msg_type == "ping":
        await websocket.send_text(json.dumps({"type": "pong", "ts_ms": int(time.time() * 1000)}))

    else:
        await websocket.send_text(json.dumps({"type": "error", "error": "unknown_type"}))


@app.websocket("/ws/{user_id}")
async def ws_endpoint(websocket: WebSocket, user_id: str):
    await manager.connect(user_id, websocket)
    await websocket.send_text(
        json.dumps({"type": "connected", "user_id": user_id, "gateway_id": GATEWAY_ID})
    )
    try:
        while True:
            raw = await websocket.receive_text()
            await _handle_frame(user_id, websocket, raw)
    except WebSocketDisconnect:
        pass
    except Exception:
        logger.exception("unhandled error on connection for %s", user_id)
    finally:
        await manager.disconnect(user_id)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request, exc):
    logger.exception("unhandled exception")
    return JSONResponse(status_code=500, content={"error": "internal_error"})
