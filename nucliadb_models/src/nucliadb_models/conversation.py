# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import json
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator

from nucliadb_models.common import CloudLink, FieldRef, FileB64
from nucliadb_models.utils import DateTime

# Shared classes


class MessageFormat(str, Enum):
    PLAIN = "PLAIN"
    HTML = "HTML"
    RST = "RST"
    MARKDOWN = "MARKDOWN"
    KEEP_MARKDOWN = "KEEP_MARKDOWN"
    JSON = "JSON"


# Visualization classes (Those used on reader endpoints)


class MessageContent(BaseModel):
    text: str | None = None
    format: MessageFormat | None = None
    attachments: list[CloudLink] | None = None
    attachments_fields: list[FieldRef] = []


class MessageType(str, Enum):
    UNSET = "UNSET"
    QUESTION = "QUESTION"
    ANSWER = "ANSWER"


class Message(BaseModel):
    timestamp: DateTime | None = None
    who: str | None = None
    to: list[str] | None = []
    content: MessageContent
    ident: str | None = None
    type_: MessageType | None = Field(None, alias="type")
    metadata: dict[str, Any] | None = None


class Conversation(BaseModel):
    """
    This is the real conversation object that will be used when visualizing
    a conversation in the field level.
    """

    total: int = Field(description="Total number of messages in the conversation")
    pages: int = Field(description="Total number of pages in the conversation")
    page: int = Field(description="Current page number")
    messages: list[Message] | None = []


class FieldConversation(BaseModel):
    """
    This is a metadata representation of a conversation about how many pages
    of messages and total of messages we have.

    This class is used mainly when exposing a conversation in the resource level
    """

    pages: int | None = None
    size: int | None = None
    total: int | None = None
    extract_strategy: str | None = None
    split_strategy: str | None = None


# Creation and update classes (Those used on writer endpoints)


class InputMessageContent(BaseModel):
    text: str = Field()
    format: MessageFormat = MessageFormat.PLAIN
    attachments: list[FileB64] = Field(default=[], max_length=50)
    attachments_fields: list[FieldRef] = Field(default=[], max_length=50)


class InputMessage(BaseModel):
    timestamp: datetime | None = Field(
        default=None, description="Time at which the message was sent, in ISO 8601 format."
    )
    who: str | None = Field(
        default=None, description="Sender of the message, e.g. 'user' or 'assistant'"
    )
    to: list[str] = Field(
        default_factory=list,
        description="List of recipients of the message, e.g. ['assistant'] or ['user']",
        max_length=100,
    )
    content: InputMessageContent
    ident: str = Field(
        description="Unique identifier for the message. Must be unique within the conversation.",
        max_length=128,
    )
    type_: MessageType | None = Field(default=None, alias="type")

    metadata: dict[str, Any] | None = Field(
        default=None,
        description="Metadata for the message. This metadata is not indexed and is only stored for retrieval purposes. It can be used to store structured information about the message content that later is serialized on retrieval results, however this metadata can not be used for searching or filtering. It can only contain up to 2kb of data.",
    )

    @field_validator("metadata", mode="before")
    @classmethod
    def validate_metadata(cls, value: dict[str, Any] | None) -> dict[str, Any] | None:
        """
        Metadata can only contain up to 2kb of data.
        """
        if value is not None:
            try:
                json_value = json.dumps(value)
            except (TypeError, ValueError) as err:
                raise ValueError(f"Metadata for message content must be JSON serializable: {err}")
            if len(json_value) > 2048:
                raise ValueError("Metadata for message content cannot exceed 2kb of data")
        return value

    @field_validator("ident", mode="after")
    @classmethod
    def validate_ident(cls, value: str) -> str:
        # The split value "0" is reserved by learning
        # Used to mark questions to override in the QA agent
        if value == "0":
            raise ValueError('Message ident cannot be "0"')
        # Ident cannot contain "/" as it is used in the text
        # block match ids (paragraph ids)
        if "/" in value:
            raise ValueError('Message ident cannot contain "/"')
        return value


class InputConversationField(BaseModel):
    messages: list[InputMessage] = Field(
        default_factory=list,
        description="List of messages in the conversation field. Each message must have a unique ident. A single conversation can contain up to 51,200 messages. You can add up to 2,048 messages per request.",
    )
    extract_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: str | None = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )

    @field_validator("messages", mode="after")
    @classmethod
    def idents_are_unique(cls, value: list[InputMessage]) -> list[InputMessage]:
        seen_idents = set()
        for message in value:
            if message.ident in seen_idents:
                raise ValueError(f'Message ident "{message.ident}" is not unique')
            seen_idents.add(message.ident)
        return value
