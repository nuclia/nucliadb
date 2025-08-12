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
from datetime import datetime
from enum import Enum
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from nucliadb_models.common import CloudLink, FieldRef, FileB64
from nucliadb_models.utils import DateTime

# Shared classes


class MessageFormat(Enum):
    PLAIN = "PLAIN"
    HTML = "HTML"
    RST = "RST"
    MARKDOWN = "MARKDOWN"
    KEEP_MARKDOWN = "KEEP_MARKDOWN"
    JSON = "JSON"


# Visualization classes (Those used on reader endpoints)


class MessageContent(BaseModel):
    text: Optional[str] = None
    format: Optional[MessageFormat] = None
    attachments: Optional[List[CloudLink]] = None
    attachments_fields: List[FieldRef] = []


class MessageType(Enum):
    UNSET = "UNSET"
    QUESTION = "QUESTION"
    ANSWER = "ANSWER"


class Message(BaseModel):
    timestamp: Optional[DateTime] = None
    who: Optional[str] = None
    to: Optional[List[str]] = []
    content: MessageContent
    ident: Optional[str] = None
    type_: Optional[MessageType] = Field(None, alias="type")


class Conversation(BaseModel):
    """
    This is the real conversation object that will be used when visualizing
    a conversation in the field level.
    """

    messages: Optional[List[Message]] = []


class FieldConversation(BaseModel):
    """
    This is a metadata representation of a conversation about how many pages
    of messages and total of messages we have.

    This class is used mainly when exposing a conversation in the resource level
    """

    pages: Optional[int] = None
    size: Optional[int] = None
    total: Optional[int] = None
    extract_strategy: Optional[str] = None
    split_strategy: Optional[str] = None


# Creation and update classes (Those used on writer endpoints)


class InputMessageContent(BaseModel):
    text: str
    format: MessageFormat = MessageFormat.PLAIN
    attachments: List[FileB64] = []
    attachments_fields: List[FieldRef] = []


class InputMessage(BaseModel):
    timestamp: Optional[datetime] = Field(
        default=None, description="Time at which the message was sent, in ISO 8601 format."
    )
    who: Optional[str] = Field(
        default=None, description="Sender of the message, e.g. 'user' or 'assistant'"
    )
    to: List[str] = Field(
        default_factory=list,
        description="List of recipients of the message, e.g. ['assistant'] or ['user']",
    )
    content: InputMessageContent
    ident: str = Field(
        description="Unique identifier for the message. Must be unique within the conversation."
    )
    type_: Optional[MessageType] = Field(None, alias="type")

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
    messages: List[InputMessage] = Field(
        default_factory=list,
        description="List of messages in the conversation field. Each message must have a unique ident.",
    )
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )
    split_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia split strategy used at processing time. If not set, the default strategy was used. Split strategies are defined at the learning configuration api.",
    )

    @field_validator("messages", mode="after")
    @classmethod
    def idents_are_unique(cls, value: List[InputMessage]) -> List[InputMessage]:
        seen_idents = set()
        for message in value:
            if message.ident in seen_idents:
                raise ValueError(f'Message ident "{message.ident}" is not unique')
            seen_idents.add(message.ident)
        return value
