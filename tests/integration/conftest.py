"""Fixtures for the integration/chaos test tier.

Unlike tests/*.py (fakeredis + moto, no containers needed), everything in
here drives the real `docker compose` stack through nginx — this is what
would have caught the pub/sub-listener-crash and nginx `index` bugs found
while building this project, since both only reproduced against the real
stack, not against fakes.

Run with the stack already up:

    docker compose up -d
    uv run pytest tests/integration -v

If the stack isn't reachable, every test here is skipped (not failed) so
`uv run pytest` (no compose stack, e.g. in CI) still passes cleanly.
"""

import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]

BASE_HTTP = os.getenv("INTEGRATION_HTTP_URL", "http://localhost:8080")
BASE_WS = os.getenv("INTEGRATION_WS_URL", "ws://localhost:8080")

GATEWAY_SERVICES = ["gateway1", "gateway2", "gateway3"]


def _stack_is_up() -> bool:
    try:
        resp = httpx.get(f"{BASE_HTTP}/healthz", timeout=2)
        return resp.status_code == 200
    except httpx.HTTPError:
        return False


@pytest.fixture(scope="session", autouse=True)
def require_stack():
    if not _stack_is_up():
        pytest.skip(
            f"docker compose stack not reachable at {BASE_HTTP} — "
            "run `docker compose up -d` first to exercise tests/integration"
        )


def ws_url(user_id: str) -> str:
    return f"{BASE_WS}/ws/{user_id}"


def http_url(path: str) -> str:
    return f"{BASE_HTTP}{path}"


def docker_compose(*args: str, timeout: int = 30) -> None:
    subprocess.run(
        ["docker", "compose", *args],
        cwd=REPO_ROOT,
        check=True,
        capture_output=True,
        timeout=timeout,
    )


@pytest.fixture
def restart_gateway_after():
    """Yields a callback the test calls with a gateway service name to stop;
    guarantees it's restarted afterward even if the test fails partway."""
    stopped: list[str] = []

    def _stop(service: str) -> None:
        docker_compose("stop", service)
        stopped.append(service)

    yield _stop

    for service in stopped:
        docker_compose("start", service)
        # nginx's default fail_timeout (10s) keeps a just-failed upstream out
        # of rotation for a while even after the container is back — wait it
        # out so later tests see all 3 replicas back in the pool rather than
        # only the survivors.
        time.sleep(12)


class DependencyOutage:
    """Test controls exactly when a shared dependency (redis, dynamodb-local)
    goes down and comes back — unlike restart_gateway_after, the test needs
    to assert behavior *during* the outage and *after* recovery within the
    same test body, not just guarantee cleanup afterward."""

    def __init__(self, service: str, ready_path: str = "/healthz"):
        self.service = service
        # /healthz only proves the gateway process is up, not that the
        # dependency behind it is actually ready — DynamoDB Local's Java
        # process in particular takes a beat to accept connections even
        # after the container reports healthy, so callers that need the
        # dependency itself confirmed ready (not just the gateway) should
        # pass a path that actually touches it.
        self.ready_path = ready_path
        self._stopped = False

    def stop(self) -> None:
        docker_compose("stop", self.service)
        self._stopped = True

    def restore(self) -> None:
        docker_compose("start", self.service)
        for _ in range(25):
            try:
                resp = httpx.get(f"{BASE_HTTP}{self.ready_path}", timeout=2)
                if resp.status_code < 500:
                    break
            except httpx.HTTPError:
                pass
            time.sleep(0.5)
        self._stopped = False


@pytest.fixture
def redis_outage():
    outage = DependencyOutage("redis")
    yield outage
    if outage._stopped:
        # safety net in case the test forgot or failed before restoring
        outage.restore()


@pytest.fixture
def dynamodb_outage():
    outage = DependencyOutage("dynamodb-local", ready_path="/rooms/_outage_readiness_probe/messages")
    yield outage
    if outage._stopped:
        outage.restore()
