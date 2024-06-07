# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.
#
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field

from nucliadb_models import CloudLink, FileB64
from nucliadb_protos import resources_pb2

_T = TypeVar("_T")


if TYPE_CHECKING:  # pragma: no cover
    MessageFormatValue = resources_pb2.MessageContent.Format.V
else:
    MessageFormatValue = int


# Shared classes


class MessageFormat(Enum):
    PLAIN = "PLAIN"
    HTML = "HTML"
    RST = "RST"
    MARKDOWN = "MARKDOWN"
    KEEP_MARKDOWN = "KEEP_MARKDOWN"


# Visualization classes (Those used on reader endpoints)


class MessageContent(BaseModel):
    text: Optional[str] = None
    format: Optional[MessageFormat] = None
    attachments: Optional[List[CloudLink]] = None


class MessageType(Enum):
    UNSET = "UNSET"
    QUESTION = "QUESTION"
    ANSWER = "ANSWER"


class Message(BaseModel):
    timestamp: Optional[datetime] = None
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

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Conversation) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class FieldConversation(BaseModel):
    """
    This is a metadata representation of a conversation about how many pages
    of messages and total of messages we have.

    This class is used mainly when exposing a conversation in the resource level
    """

    pages: Optional[int] = None
    size: Optional[int] = None
    total: Optional[int] = None

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.FieldConversation) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


# Creation and update classes (Those used on writer endpoints)


class InputMessageContent(BaseModel):
    text: str
    format: MessageFormat = MessageFormat.PLAIN
    attachments: List[FileB64] = []


class InputMessage(BaseModel):
    timestamp: Optional[datetime] = None
    who: Optional[str] = None
    to: List[str] = []
    content: InputMessageContent
    ident: str
    type_: Optional[MessageType] = Field(None, alias="type")


class InputConversationField(BaseModel):
    messages: List[InputMessage] = []


# Processing classes (Those used to sent to push endpoints)


class PushMessageFormat(MessageFormatValue, Enum):  # type: ignore
    PLAIN = 0
    HTML = 1
    MARKDOWN = 2
    RST = 3


class PushMessageContent(BaseModel):
    text: Optional[str] = None
    format: PushMessageFormat
    attachments: List[str] = []


class PushMessage(BaseModel):
    timestamp: Optional[datetime] = None
    who: Optional[str] = None
    to: List[str] = []
    content: PushMessageContent
    ident: str


class PushConversation(BaseModel):
    messages: List[PushMessage] = []
