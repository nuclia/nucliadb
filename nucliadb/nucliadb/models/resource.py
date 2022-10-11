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
from typing import Dict, List, Optional, Union

from pydantic import BaseModel, validator

from nucliadb.models.conversation import FieldConversation
from nucliadb.models.datetime import FieldDatetime
from nucliadb.models.extracted import (
    ExtractedText,
    FieldComputedMetadata,
    FileExtractedData,
    LargeComputedMetadata,
    LinkExtractedData,
    VectorObject,
)
from nucliadb.models.file import FieldFile
from nucliadb.models.keywordset import FieldKeywordset
from nucliadb.models.layout import FieldLayout
from nucliadb.models.link import FieldLink
from nucliadb.models.metadata import (
    Metadata,
    Origin,
    Relation,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb.models.text import FieldText
from nucliadb.models.utils import SlugString


class NucliaDBRoles(str, Enum):
    MANAGER = "MANAGER"
    READER = "READER"
    WRITER = "WRITER"


class KnowledgeBoxConfig(BaseModel):
    slug: Optional[SlugString] = None
    title: Optional[str] = None
    description: Optional[str] = None
    enabled_filters: List[str] = []
    enabled_insights: List[str] = []
    disable_vectors: bool = False

    @validator("slug")
    def id_check(cls, v: str) -> str:

        for char in v:
            if char in string.ascii_uppercase:
                raise ValueError("No uppercase ID")
            if char in "&@ /\\ ":
                raise ValueError("Invalid chars")

        return v


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


class FileFieldData(BaseModel):
    value: Optional[FieldFile]
    extracted: Optional[FileFieldExtractedData]


class LinkFieldData(BaseModel):
    value: Optional[FieldLink]
    extracted: Optional[LinkFieldExtractedData]
    error: Optional[Error]


class LayoutFieldData(BaseModel):
    value: Optional[FieldLayout]
    extracted: Optional[LayoutFieldExtractedData]


class ConversationFieldData(BaseModel):
    value: Optional[FieldConversation]
    extracted: Optional[ConversationFieldExtractedData]


class KeywordsetFieldData(BaseModel):
    value: Optional[FieldKeywordset]
    extracted: Optional[KeywordsetFieldExtractedData]


class DatetimeFieldData(BaseModel):
    value: Optional[FieldDatetime]
    extracted: Optional[DatetimeFieldExtractedData]


class ResourceData(BaseModel):
    texts: Optional[Dict[str, TextFieldData]]
    files: Optional[Dict[str, FileFieldData]]
    links: Optional[Dict[str, LinkFieldData]]
    layouts: Optional[Dict[str, LayoutFieldData]]
    conversations: Optional[Dict[str, ConversationFieldData]]
    keywordsets: Optional[Dict[str, KeywordsetFieldData]]
    datetimes: Optional[Dict[str, DatetimeFieldData]]


class Resource(BaseModel):
    id: str

    # This first block of attributes correspond to Basic fields
    title: Optional[str]
    summary: Optional[str]
    icon: Optional[str]
    layout: Optional[str]
    thumbnail: Optional[str]
    metadata: Optional[Metadata]
    usermetadata: Optional[UserMetadata]
    fieldmetadata: Optional[List[UserFieldMetadata]]
    created: Optional[datetime]
    modified: Optional[datetime]
    last_seqid: Optional[int]

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
