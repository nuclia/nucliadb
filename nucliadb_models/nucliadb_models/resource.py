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

import string
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Type, TypeVar, Union

from google.protobuf.json_format import MessageToDict
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxConfig as PBKnowledgeBoxConfig
from pydantic import BaseModel, validator

from nucliadb_models.conversation import FieldConversation
from nucliadb_models.datetime import FieldDatetime
from nucliadb_models.extracted import (
    ExtractedText,
    FieldComputedMetadata,
    FileExtractedData,
    LargeComputedMetadata,
    LinkExtractedData,
    VectorObject,
)
from nucliadb_models.file import FieldFile
from nucliadb_models.keywordset import FieldKeywordset
from nucliadb_models.layout import FieldLayout
from nucliadb_models.link import FieldLink
from nucliadb_models.metadata import (
    ComputedMetadata,
    Metadata,
    Origin,
    Relation,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.text import FieldText
from nucliadb_models.utils import SlugString
from nucliadb_models.vectors import UserVectorSet, VectorSimilarity

_T = TypeVar("_T")


class NucliaDBRoles(str, Enum):
    MANAGER = "MANAGER"
    READER = "READER"
    WRITER = "WRITER"


class ResourceFieldProperties(str, Enum):
    VALUE = "value"
    EXTRACTED = "extracted"
    ERROR = "error"


class ExtractedDataTypeName(str, Enum):
    TEXT = "text"
    METADATA = "metadata"
    SHORTENED_METADATA = "shortened_metadata"
    LARGE_METADATA = "large_metadata"
    VECTOR = "vectors"
    LINK = "link"
    FILE = "file"
    USERVECTORS = "uservectors"


class KnowledgeBoxConfig(BaseModel):
    slug: Optional[SlugString] = None
    title: Optional[str] = None
    description: Optional[str] = None
    enabled_filters: List[str] = []
    enabled_insights: List[str] = []
    disable_vectors: bool = False
    similarity: Optional[VectorSimilarity]

    @validator("slug")
    def id_check(cls, v: str) -> str:
        for char in v:
            if char in string.ascii_uppercase:
                raise ValueError("No uppercase ID")
            if char in "&@ /\\ ":
                raise ValueError("Invalid chars")
        return v

    @classmethod
    def from_message(cls: Type[_T], message: PBKnowledgeBoxConfig) -> _T:
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        return cls(**as_dict)


class KnowledgeBoxObjSummary(BaseModel):
    slug: Optional[SlugString] = None
    uuid: str


class KnowledgeBoxObjID(BaseModel):
    uuid: str


class KnowledgeBoxObj(BaseModel):
    slug: Optional[SlugString] = None
    uuid: str
    config: Optional[KnowledgeBoxConfig] = None


class KnowledgeBoxList(BaseModel):
    kbs: List[KnowledgeBoxObjSummary] = []


# Resources


class ExtractedData(BaseModel):
    text: Optional[ExtractedText]
    metadata: Optional[FieldComputedMetadata]
    large_metadata: Optional[LargeComputedMetadata]
    vectors: Optional[VectorObject]
    uservectors: Optional[UserVectorSet]


class TextFieldExtractedData(ExtractedData):
    pass


class FileFieldExtractedData(ExtractedData):
    file: Optional[FileExtractedData]


class LinkFieldExtractedData(ExtractedData):
    link: Optional[LinkExtractedData]


class LayoutFieldExtractedData(ExtractedData):
    pass


class ConversationFieldExtractedData(ExtractedData):
    pass


class KeywordsetFieldExtractedData(ExtractedData):
    pass


class DatetimeFieldExtractedData(ExtractedData):
    pass


ExtractedDataType = Optional[
    Union[
        TextFieldExtractedData,
        FileFieldExtractedData,
        LinkFieldExtractedData,
        LayoutFieldExtractedData,
        ConversationFieldExtractedData,
        KeywordsetFieldExtractedData,
        DatetimeFieldExtractedData,
    ]
]


class Error(BaseModel):
    body: str
    code: int


class FieldData(BaseModel):
    ...


class TextFieldData(BaseModel):
    value: Optional[FieldText]
    extracted: Optional[TextFieldExtractedData]
    error: Optional[Error]


class FileFieldData(BaseModel):
    value: Optional[FieldFile]
    extracted: Optional[FileFieldExtractedData]
    error: Optional[Error]


class LinkFieldData(BaseModel):
    value: Optional[FieldLink]
    extracted: Optional[LinkFieldExtractedData]
    error: Optional[Error]


class LayoutFieldData(BaseModel):
    value: Optional[FieldLayout]
    extracted: Optional[LayoutFieldExtractedData]
    error: Optional[Error]


class ConversationFieldData(BaseModel):
    value: Optional[FieldConversation]
    extracted: Optional[ConversationFieldExtractedData]
    error: Optional[Error]


class KeywordsetFieldData(BaseModel):
    value: Optional[FieldKeywordset]
    extracted: Optional[KeywordsetFieldExtractedData]
    error: Optional[Error]


class DatetimeFieldData(BaseModel):
    value: Optional[FieldDatetime]
    extracted: Optional[DatetimeFieldExtractedData]
    error: Optional[Error]


class GenericFieldData(BaseModel):
    value: Optional[str]
    extracted: Optional[TextFieldExtractedData]
    error: Optional[Error]


class ResourceData(BaseModel):
    texts: Optional[Dict[str, TextFieldData]]
    files: Optional[Dict[str, FileFieldData]]
    links: Optional[Dict[str, LinkFieldData]]
    layouts: Optional[Dict[str, LayoutFieldData]]
    conversations: Optional[Dict[str, ConversationFieldData]]
    keywordsets: Optional[Dict[str, KeywordsetFieldData]]
    datetimes: Optional[Dict[str, DatetimeFieldData]]
    generics: Optional[Dict[str, GenericFieldData]]


class QueueType(str, Enum):  # type: ignore
    PRIVATE = "private"
    SHARED = "shared"


class Resource(BaseModel):
    id: str

    # This first block of attributes correspond to Basic fields
    slug: Optional[str]
    title: Optional[str]
    summary: Optional[str]
    icon: Optional[str]
    layout: Optional[str]
    thumbnail: Optional[str]
    metadata: Optional[Metadata]
    usermetadata: Optional[UserMetadata]
    fieldmetadata: Optional[List[UserFieldMetadata]]
    computedmetadata: Optional[ComputedMetadata]
    created: Optional[datetime]
    modified: Optional[datetime]
    last_seqid: Optional[int]
    last_account_seq: Optional[int]
    queue: Optional[QueueType]

    origin: Optional[Origin]
    relations: Optional[List[Relation]]

    data: Optional[ResourceData]


class ResourcePagination(BaseModel):
    page: int
    size: int
    last: bool


class ResourceList(BaseModel):
    resources: List[Resource]
    pagination: ResourcePagination
