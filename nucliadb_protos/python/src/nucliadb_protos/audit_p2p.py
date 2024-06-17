# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.6.2](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.3
# Pydantic Version: 1.10.14
import typing
from datetime import datetime
from enum import IntEnum

from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel, Field

from .nodereader_p2p import SearchRequest
from .resources_p2p import FieldID, FieldType


class ClientType(IntEnum):
    API = 0
    WEB = 1
    WIDGET = 2
    DESKTOP = 3
    DASHBOARD = 4
    CHROME_EXTENSION = 5


class AuditField(BaseModel):
    class FieldAction(IntEnum):
        ADDED = 0
        MODIFIED = 1
        DELETED = 2

    action: "AuditField.FieldAction" = Field(default=0)
    size: int = Field(default=0)
    size_delta: int = Field(default=0)
    field_id: str = Field(default="")
    field_type: FieldType = Field(default=0)
    filename: str = Field(default="")


class AuditKBCounter(BaseModel):
    paragraphs: int = Field(default=0)
    fields: int = Field(default=0)


class ChatContext(BaseModel):
    author: str = Field(default="")
    text: str = Field(default="")


class ChatAudit(BaseModel):
    question: str = Field(default="")
    answer: typing.Optional[str] = Field(default="")
    rephrased_question: typing.Optional[str] = Field(default="")
    context: typing.List[ChatContext] = Field(default_factory=list)
    learning_id: str = Field(default="")


class AuditRequest(BaseModel):
    class AuditType(IntEnum):
        VISITED = 0
        MODIFIED = 1
        DELETED = 2
        NEW = 3
        STARTED = 4
        STOPPED = 5
        SEARCH = 6
        PROCESSED = 7
        KB_DELETED = 8
        SUGGEST = 9
        INDEXED = 10
        CHAT = 11

    type: "AuditRequest.AuditType" = Field(default=0)
    kbid: str = Field(default="")
    userid: str = Field(default="")
    time: datetime = Field(default_factory=datetime.now)
    fields: typing.List[str] = Field(default_factory=list)
    search: SearchRequest = Field()
    timeit: float = Field(default=0.0)
    origin: str = Field(default="")
    rid: str = Field(default="")
    task: str = Field(default="")
    resources: int = Field(default=0)
    field_metadata: typing.List[FieldID] = Field(default_factory=list)
    fields_audit: typing.List[AuditField] = Field(default_factory=list)
    client_type: ClientType = Field(default=0)
    trace_id: str = Field(default="")
    kb_counter: AuditKBCounter = Field()
    chat: ChatAudit = Field()
    success: bool = Field(default=False)
