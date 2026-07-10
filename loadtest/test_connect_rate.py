"""Measures how fast the LB + gateway fleet can accept new WebSocket
connections: per-connection handshake latency and aggregate connections/sec.
"""

import asyncio
import os
import time

import websockets

from loadtest.common import Stats, new_user_id, print_table, ws_url

NUM_CONNECTIONS = int(os.getenv("CONNECT_RATE_N", "500"))
CONCURRENCY = int(os.getenv("CONNECT_RATE_CONCURRENCY", "100"))


async def _connect_one(sem: asyncio.Semaphore) -> tuple[float, "websockets.WebSocketClientProtocol"]:
    user_id = new_user_id("connrate")
    async with sem:
        t0 = time.perf_counter()
        ws = await websockets.connect(ws_url(user_id), open_timeout=10)
        t1 = time.perf_counter()
    return (t1 - t0) * 1000, ws


async def run() -> tuple[Stats, float]:
    sem = asyncio.Semaphore(CONCURRENCY)
    wall_start = time.perf_counter()
    results = await asyncio.gather(*[_connect_one(sem) for _ in range(NUM_CONNECTIONS)])
    wall_elapsed = time.perf_counter() - wall_start

    latencies = [r[0] for r in results]
    sockets = [r[1] for r in results]

    conns_per_sec = NUM_CONNECTIONS / wall_elapsed

    await asyncio.gather(*[s.close() for s in sockets], return_exceptions=True)

    return Stats("Connection accept latency", latencies), conns_per_sec


async def main() -> None:
    stats, conns_per_sec = await run()
    print_table([stats], {"Connections/sec (accept rate)": conns_per_sec})


if __name__ == "__main__":
    asyncio.run(main())
