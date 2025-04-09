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


# Processing classes (Those used to sent to push endpoints)


from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Optional

from pydantic import BaseModel, Field

from nucliadb_models.processing import PushProcessingOptions
from nucliadb_models.resource import QueueType
from nucliadb_protos.resources_pb2 import CloudFile

if TYPE_CHECKING:  # pragma: no cover
    SourceValue = CloudFile.Source.V
else:
    SourceValue = int


class ClassificationLabel(BaseModel):
    """
    NOTE: This model is used to send the labels of each field in the processing requests.
    It is a rath is not meant to be used by api users.
    """

    labelset: str
    label: str

    def __hash__(self):
        return hash((self.labelset, self.label))


class PushTextFormat(int, Enum):
    PLAIN = 0
    HTML = 1
    MARKDOWN = 2
    RST = 3
    JSON = 4
    KEEP_MARKDOWN = 5
    JSONL = 6
    PLAIN_BLANKLINE_SPLIT = 7


class Text(BaseModel):
    body: str
    format: PushTextFormat
    extract_strategy: Optional[str] = None
    classification_labels: list[ClassificationLabel] = []


class LinkUpload(BaseModel):
    link: str
    headers: dict[str, str] = {}
    cookies: dict[str, str] = {}
    localstorage: dict[str, str] = {}
    css_selector: Optional[str] = Field(
        None,
        title="Css selector",
        description="Css selector to parse the link",
    )
    xpath: Optional[str] = Field(
        None,
        title="Xpath",
        description="Xpath to parse the link",
    )
    extract_strategy: Optional[str] = None
    classification_labels: list[ClassificationLabel] = []


class PushMessageFormat(int, Enum):
    PLAIN = 0
    HTML = 1
    MARKDOWN = 2
    RST = 3
    JSON = 4


class PushMessageContent(BaseModel):
    text: Optional[str] = None
    format: PushMessageFormat
    attachments: list[str] = []


class PushMessage(BaseModel):
    timestamp: Optional[datetime] = None
    who: Optional[str] = None
    to: list[str] = []
    content: PushMessageContent
    ident: str


class PushConversation(BaseModel):
    messages: list[PushMessage] = []
    extract_strategy: Optional[str] = None
    classification_labels: list[ClassificationLabel] = []


class Source(SourceValue, Enum):  # type: ignore
    HTTP = 0
    INGEST = 1


class ProcessingInfo(BaseModel):
    seqid: Optional[int] = None
    account_seq: Optional[int] = None
    queue: Optional[QueueType] = None


class PushPayload(BaseModel):
    uuid: str
    slug: Optional[str] = None
    kbid: str
    source: Optional[Source] = None
    userid: str

    title: Optional[str] = None

    genericfield: dict[str, Text] = {}

    # New File
    filefield: dict[str, str] = Field(
        default={},
        description="Map of each file field to the jwt token computed in ProcessingEngine methods",
    )

    # New Link
    linkfield: dict[str, LinkUpload] = {}

    # Diff on Text Field
    textfield: dict[str, Text] = {}

    # New conversations to process
    conversationfield: dict[str, PushConversation] = {}

    # Only internal
    partition: int

    # List of available processing options (with default values)
    processing_options: Optional[PushProcessingOptions] = Field(default_factory=PushProcessingOptions)
