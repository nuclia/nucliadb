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
from typing import List, Optional

from pydantic import BaseModel, Field, field_validator

from nucliadb_models import CloudLink, FieldRef, FileB64
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


# Creation and update classes (Those used on writer endpoints)


class InputMessageContent(BaseModel):
    text: str
    format: MessageFormat = MessageFormat.PLAIN
    attachments: List[FileB64] = []
    attachments_fields: List[FieldRef] = []


class InputMessage(BaseModel):
    timestamp: Optional[datetime] = None
    who: Optional[str] = None
    to: List[str] = []
    content: InputMessageContent
    ident: str
    type_: Optional[MessageType] = Field(None, alias="type")

    @field_validator("ident", mode="after")
    @classmethod
    def ident_not_zero(cls, value: str) -> str:
        # The split value "0" is reserved by learning
        # Used to mark questions to override in the QA agent
        if value == "0":
            raise ValueError('Message ident cannot be "0"')
        return value


class InputConversationField(BaseModel):
    messages: List[InputMessage] = []
    extract_strategy: Optional[str] = Field(
        default=None,
        description="Id of the Nuclia extract strategy used at processing time. If not set, the default strategy was used. Extract strategies are defined at the learning configuration api.",
    )
