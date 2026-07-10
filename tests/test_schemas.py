import pytest
from pydantic import ValidationError

from src.schemas import JoinRoom, LeaveRoom, Ping, SendMessage


def test_join_room_requires_room_id():
    with pytest.raises(ValidationError):
        JoinRoom(type="join")

    assert JoinRoom(type="join", room_id="general").room_id == "general"


def test_leave_room_rejects_wrong_type():
    with pytest.raises(ValidationError):
        LeaveRoom(type="join", room_id="general")


def test_send_message_optional_fields_default_none():
    msg = SendMessage(type="send", room_id="general", text="hi")
    assert msg.client_msg_id is None
    assert msg.sent_at_ms is None


def test_ping_type_literal():
    assert Ping(type="ping").type == "ping"
    with pytest.raises(ValidationError):
        Ping(type="pong")
