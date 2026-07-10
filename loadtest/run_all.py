"""Runs the full load test suite against the gateway fleet (through nginx by
default) and prints one consolidated benchmark table.

Usage:
    uv run python -m loadtest.run_all
    GATEWAY_URL=ws://localhost:8080 uv run python -m loadtest.run_all
"""

import asyncio

from loadtest import test_churn, test_connect_rate, test_fanout_latency
from loadtest.common import print_table


async def main() -> None:
    print("== connection accept rate ==")
    connect_stats, conns_per_sec = await test_connect_rate.run()
    print_table([connect_stats], {"Connections/sec (accept rate)": conns_per_sec})
    print()

    print("== cross-instance fanout latency ==")
    fanout_stats = await test_fanout_latency.run()
    delivered = fanout_stats.count
    expected = test_fanout_latency.NUM_RECEIVERS * test_fanout_latency.NUM_MESSAGES
    print_table([fanout_stats], {"Delivered / expected": delivered / expected if expected else 0.0})
    print()

    print("== subscribe/unsubscribe churn cost ==")
    join_stats, leave_stats = await test_churn.run()
    print_table([join_stats, leave_stats])
    print()

    print("== combined benchmark table ==")
    print_table(
        [connect_stats, fanout_stats, join_stats, leave_stats],
        {
            "Connections/sec (accept rate)": conns_per_sec,
            "Fanout delivered / expected": delivered / expected if expected else 0.0,
        },
    )


if __name__ == "__main__":
    asyncio.run(main())
