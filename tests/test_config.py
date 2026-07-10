from src.config import presence_key, registry_key, room_channel, user_channel


def test_room_channel_naming():
    assert room_channel("general") == "chat:room:general"


def test_user_channel_naming():
    assert user_channel("alice") == "chat:user:alice"


def test_presence_key_naming():
    assert presence_key("alice") == "presence:user:alice"


def test_registry_key_naming():
    assert registry_key("alice") == "registry:user:alice"
