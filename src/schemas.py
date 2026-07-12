from typing import Literal

from pydantic import BaseModel


class JoinRoom(BaseModel):
    type: Literal["join"]
    room_id: str


class LeaveRoom(BaseModel):
    type: Literal["leave"]
    room_id: str


class SendMessage(BaseModel):
    type: Literal["send"]
    room_id: str
    text: str
    client_msg_id: str | None = None
    sent_at_ms: int | None = None


class Ping(BaseModel):
    type: Literal["ping"]


class MarkRead(BaseModel):
    type: Literal["mark_read"]
    room_id: str
    sort_key: str
