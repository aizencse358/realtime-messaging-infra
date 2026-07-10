import os
import uuid

from dotenv import load_dotenv

load_dotenv()

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

DYNAMODB_ENDPOINT_URL = os.getenv("DYNAMODB_ENDPOINT_URL", "http://localhost:8000")
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "local")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "local")
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

PRESENCE_TTL_SECONDS = int(os.getenv("PRESENCE_TTL_SECONDS", "45"))
PRESENCE_HEARTBEAT_SECONDS = int(os.getenv("PRESENCE_HEARTBEAT_SECONDS", "20"))
REGISTRY_TTL_SECONDS = int(os.getenv("REGISTRY_TTL_SECONDS", "45"))

# Unique per gateway process. GATEWAY_ID is set per-replica in docker-compose; a
# random suffix is appended so two instances launched with the same env value
# (e.g. local dev, no compose) never collide in the registry.
GATEWAY_ID = f"{os.getenv('GATEWAY_ID', 'gateway')}-{uuid.uuid4().hex[:8]}"

MESSAGES_TABLE = os.getenv("MESSAGES_TABLE", "Messages")
USERS_TABLE = os.getenv("USERS_TABLE", "Users")
ROOM_MEMBERS_TABLE = os.getenv("ROOM_MEMBERS_TABLE", "RoomMembers")


def room_channel(room_id: str) -> str:
    return f"chat:room:{room_id}"


def user_channel(user_id: str) -> str:
    return f"chat:user:{user_id}"


def presence_key(user_id: str) -> str:
    return f"presence:user:{user_id}"


def registry_key(user_id: str) -> str:
    return f"registry:user:{user_id}"
