"""Measures end-to-end fanout latency for a message published in one room:
one sender publishes, N receivers (connected round-robin across gateway
replicas via nginx, so most receive the message from an instance other than
the sender's) record delivery latency. This is the number that proves cross-
instance pub/sub fanout actually works and is fast.
"""

import asyncio
import json
import os
import time

import websockets

from loadtest.common import Stats, new_user_id, print_table, ws_url

NUM_RECEIVERS = int(os.getenv("FANOUT_RECEIVERS", "20"))
NUM_MESSAGES = int(os.getenv("FANOUT_MESSAGES", "100"))
ROOM_ID = "loadtest-fanout-room"


async def _open_and_join(user_id: str) -> "websockets.WebSocketClientProtocol":
    ws = await websockets.connect(ws_url(user_id), open_timeout=10)
    await ws.send(json.dumps({"type": "join", "room_id": ROOM_ID}))
    ack = json.loads(await ws.recv())
    assert ack["type"] == "joined"
    return ws


async def _drain_until_message(ws, client_msg_id: str) -> float | None:
    """Read frames until we see the broadcast message for client_msg_id, or
    give up after a short timeout. Returns receipt time (perf_counter-ish
    wall clock ms) or None on timeout."""
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
        except asyncio.TimeoutError:
            return None
        frame = json.loads(raw)
        if frame.get("type") == "message" and frame.get("client_msg_id") == client_msg_id:
            return time.time() * 1000
    return None


async def run() -> Stats:
    sender_ws = await _open_and_join(new_user_id("fanout-sender"))
    receiver_sockets = await asyncio.gather(
        *[_open_and_join(new_user_id("fanout-recv")) for _ in range(NUM_RECEIVERS)]
    )

    all_latencies: list[float] = []

    for i in range(NUM_MESSAGES):
        client_msg_id = f"msg-{i}"
        sent_at_ms = time.time() * 1000

        recv_tasks = [
            asyncio.create_task(_drain_until_message(rws, client_msg_id))
            for rws in receiver_sockets
        ]

        await sender_ws.send(
            json.dumps(
                {
                    "type": "send",
                    "room_id": ROOM_ID,
                    "text": f"hello {i}",
                    "client_msg_id": client_msg_id,
                    "sent_at_ms": sent_at_ms,
                }
            )
        )
        # drain the sender's own "sent" ack so it doesn't pollute later reads
        await sender_ws.recv()

        receipts = await asyncio.gather(*recv_tasks)
        for receipt_ms in receipts:
            if receipt_ms is not None:
                all_latencies.append(receipt_ms - sent_at_ms)

    await sender_ws.close()
    await asyncio.gather(*[rws.close() for rws in receiver_sockets], return_exceptions=True)

    return Stats("Cross-instance fanout latency", all_latencies)


async def main() -> None:
    stats = await run()
    delivered = stats.count
    expected = NUM_RECEIVERS * NUM_MESSAGES
    print_table([stats], {"Delivered / expected": delivered / expected if expected else 0.0})


if __name__ == "__main__":
    asyncio.run(main())
