"""One-shot entrypoint: create DynamoDB tables if they don't already exist.

Run as its own compose service before the gateways start, and idempotent
enough to also run (again, harmlessly) inside each gateway's startup hook.
"""

import logging
import time

from botocore.exceptions import EndpointConnectionError

from src.dynamo import init_tables_sync

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("init_tables")


def main() -> None:
    attempts = 0
    while True:
        attempts += 1
        try:
            init_tables_sync()
            logger.info("DynamoDB tables ready (Messages, Users, RoomMembers)")
            return
        except EndpointConnectionError:
            if attempts >= 30:
                raise
            logger.info("dynamodb-local not ready yet, retrying (%d/30)...", attempts)
            time.sleep(2)


if __name__ == "__main__":
    main()
