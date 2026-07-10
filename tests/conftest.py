import boto3
import fakeredis.aioredis
import pytest
from moto import mock_aws

from src import connection_manager as connection_manager_module
from src import dynamo as dynamo_module
from src import redis_client as redis_client_module


@pytest.fixture
def fake_redis(monkeypatch):
    """A fresh in-memory fake Redis, wired in as the shared client used by
    both src.redis_client.get_redis() and connection_manager's module-level
    import of it."""
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(redis_client_module, "_client", client)
    monkeypatch.setattr(connection_manager_module, "get_redis", lambda: client)
    yield client


@pytest.fixture
def dynamo_tables(monkeypatch):
    """Real DynamoDB API semantics via moto, without touching the network or
    requiring dynamodb-local to be running."""
    with mock_aws():
        monkeypatch.setattr(
            dynamo_module,
            "_resource",
            lambda: boto3.resource("dynamodb", region_name="us-east-1"),
        )
        dynamo_module.init_tables_sync()
        yield
