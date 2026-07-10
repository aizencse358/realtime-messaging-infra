"""Measures the cost of room subscription churn: each join/leave below uses a
brand-new room_id, so the connected client is always the first (and then
last) local member, forcing an actual Redis SUBSCRIBE/UNSUBSCRIBE on the
gateway for every operation rather than a cheap refcount bump.
"""

import asyncio
import json
import os
import time
import uuid

import websockets

from loadtest.common import Stats, new_user_id, print_table, ws_url

NUM_ROOMS = int(os.getenv("CHURN_ROOMS", "300"))


async def run() -> tuple[Stats, Stats]:
    ws = await websockets.connect(ws_url(new_user_id("churn")), open_timeout=10)

    join_latencies: list[float] = []
    leave_latencies: list[float] = []

    for _ in range(NUM_ROOMS):
        room_id = f"churn-room-{uuid.uuid4().hex[:12]}"

        t0 = time.perf_counter()
        await ws.send(json.dumps({"type": "join", "room_id": room_id}))
        ack = json.loads(await ws.recv())
        t1 = time.perf_counter()
        assert ack["type"] == "joined"
        join_latencies.append((t1 - t0) * 1000)

        t0 = time.perf_counter()
        await ws.send(json.dumps({"type": "leave", "room_id": room_id}))
        ack = json.loads(await ws.recv())
        t1 = time.perf_counter()
        assert ack["type"] == "left"
        leave_latencies.append((t1 - t0) * 1000)

    await ws.close()

    return (
        Stats("Room join (subscribe) RTT", join_latencies),
        Stats("Room leave (unsubscribe) RTT", leave_latencies),
    )


async def main() -> None:
    join_stats, leave_stats = await run()
    print_table([join_stats, leave_stats])


if __name__ == "__main__":
    asyncio.run(main())
