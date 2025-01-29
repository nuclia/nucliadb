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
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator

from nucliadb_models.common import FieldTypeName
from nucliadb_models.conversation import Conversation, FieldConversation
from nucliadb_models.external_index_providers import ExternalIndexProvider
from nucliadb_models.extracted import (
    ExtractedText,
    FieldComputedMetadata,
    FieldQuestionAnswers,
    FileExtractedData,
    LargeComputedMetadata,
    LinkExtractedData,
    VectorObject,
)
from nucliadb_models.file import FieldFile
from nucliadb_models.link import FieldLink
from nucliadb_models.metadata import (
    ComputedMetadata,
    Extra,
    Metadata,
    Origin,
    Relation,
    UserFieldMetadata,
    UserMetadata,
)
from nucliadb_models.security import ResourceSecurity
from nucliadb_models.text import FieldText
from nucliadb_models.utils import SlugString
from nucliadb_models.vectors import SemanticModelMetadata, VectorSimilarity


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
    QA = "question_answers"


class ReleaseChannel(str, Enum):
    """
    Deprecated. No longer used.
    """

    STABLE = "STABLE"
    EXPERIMENTAL = "EXPERIMENTAL"


class KnowledgeBoxConfig(BaseModel):
    slug: Optional[SlugString] = Field(
        default=None, title="Slug", description="Slug for the Knowledge Box."
    )
    title: Optional[str] = Field(default=None, title="Title", description="Title for the Knowledge Box.")
    description: Optional[str] = Field(
        default=None,
        title="Description",
        description="Description for the Knowledge Box.",
    )
    learning_configuration: Optional[Dict[str, Any]] = Field(
        default=None,
        title="Learning Configuration",
        description="Learning configuration for the Knowledge Box. If provided, NucliaDB will set the learning configuration for the Knowledge Box.",  # noqa: E501
    )

    external_index_provider: Optional[ExternalIndexProvider] = Field(
        default=None,
        title="External Index Provider",
        description="External index provider for the Knowledge Box.",
    )

    configured_external_index_provider: Optional[dict[str, Any]] = Field(
        default=None,
        title="Configured External Index Provider",
        description="Metadata for the configured external index provider (if any)",
    )

    similarity: Optional[VectorSimilarity] = Field(
        default=None,
        description="This field is deprecated. Use 'learning_configuration' instead.",
    )

    hidden_resources_enabled: bool = Field(
        default=False,
        description="Allow hiding resources",
    )

    hidden_resources_hide_on_creation: bool = Field(
        default=False,
        description="Hide newly created resources",
    )

    @field_validator("slug")
    @classmethod
    def id_check(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return v

        for char in v:
            if char in string.ascii_uppercase:
                raise ValueError("No uppercase ID")
            if char in "&@ /\\ ":
                raise ValueError("Invalid chars")
        return v

    @model_validator(mode="after")
    def check_hidden_resources(self) -> "KnowledgeBoxConfig":
        if self.hidden_resources_hide_on_creation and not self.hidden_resources_enabled:
            raise ValueError("Cannot hide new resources if the hidden resources feature is disabled")
        return self


class KnowledgeBoxObjSummary(BaseModel):
    slug: Optional[SlugString] = None
    uuid: str


class KnowledgeBoxObjID(BaseModel):
    uuid: str


class KnowledgeBoxObj(BaseModel):
    """
    The API representation of a Knowledge Box object.
    """

    slug: Optional[SlugString] = None
    uuid: str
    config: Optional[KnowledgeBoxConfig] = None
    model: Optional[SemanticModelMetadata] = None


class KnowledgeBoxList(BaseModel):
    kbs: List[KnowledgeBoxObjSummary] = []


# Resources


class ExtractedData(BaseModel):
    text: Optional[ExtractedText] = None
    metadata: Optional[FieldComputedMetadata] = None
    large_metadata: Optional[LargeComputedMetadata] = None
    vectors: Optional[VectorObject] = None
    question_answers: Optional[FieldQuestionAnswers] = None


class TextFieldExtractedData(ExtractedData):
    pass


class FileFieldExtractedData(ExtractedData):
    file: Optional[FileExtractedData] = None


class LinkFieldExtractedData(ExtractedData):
    link: Optional[LinkExtractedData] = None


class ConversationFieldExtractedData(ExtractedData):
    pass


ExtractedDataType = Optional[
    Union[
        TextFieldExtractedData,
        FileFieldExtractedData,
        LinkFieldExtractedData,
        ConversationFieldExtractedData,
    ]
]


class Error(BaseModel):
    body: str
    code: int
    code_str: str
    created: Optional[datetime]


class FieldData(BaseModel): ...


class TextFieldData(BaseModel):
    value: Optional[FieldText] = None
    extracted: Optional[TextFieldExtractedData] = None
    error: Optional[Error] = None
    status: Optional[str] = None
    errors: Optional[list[Error]] = None


class FileFieldData(BaseModel):
    value: Optional[FieldFile] = None
    extracted: Optional[FileFieldExtractedData] = None
    error: Optional[Error] = None
    status: Optional[str] = None
    errors: Optional[list[Error]] = None


class LinkFieldData(BaseModel):
    value: Optional[FieldLink] = None
    extracted: Optional[LinkFieldExtractedData] = None
    error: Optional[Error] = None
    status: Optional[str] = None
    errors: Optional[list[Error]] = None


class ConversationFieldData(BaseModel):
    value: Optional[FieldConversation] = None
    extracted: Optional[ConversationFieldExtractedData] = None
    error: Optional[Error] = None
    status: Optional[str] = None
    errors: Optional[list[Error]] = None


class GenericFieldData(BaseModel):
    value: Optional[str] = None
    extracted: Optional[TextFieldExtractedData] = None
    error: Optional[Error] = None
    status: Optional[str] = None
    errors: Optional[list[Error]] = None


class ResourceData(BaseModel):
    texts: Optional[Dict[str, TextFieldData]] = None
    files: Optional[Dict[str, FileFieldData]] = None
    links: Optional[Dict[str, LinkFieldData]] = None
    conversations: Optional[Dict[str, ConversationFieldData]] = None
    generics: Optional[Dict[str, GenericFieldData]] = None


class QueueType(str, Enum):
    PRIVATE = "private"
    SHARED = "shared"


class Resource(BaseModel):
    id: str

    # This first block of attributes correspond to Basic fields
    slug: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    icon: Optional[str] = None
    thumbnail: Optional[str] = None
    metadata: Optional[Metadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    computedmetadata: Optional[ComputedMetadata] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    last_seqid: Optional[int] = None
    last_account_seq: Optional[int] = None
    queue: Optional[QueueType] = None
    hidden: Optional[bool] = None

    origin: Optional[Origin] = None
    extra: Optional[Extra] = None
    relations: Optional[List[Relation]] = None

    data: Optional[ResourceData] = None

    security: Optional[ResourceSecurity] = Field(
        default=None,
        title="Security",
        description="Resource security metadata",
    )


class ResourcePagination(BaseModel):
    page: int
    size: int
    last: bool


class ResourceList(BaseModel):
    resources: List[Resource]
    pagination: ResourcePagination


class ResourceField(BaseModel):
    field_type: FieldTypeName
    field_id: str
    value: Optional[
        Union[
            FieldText,
            FieldFile,
            FieldLink,
            Conversation,
        ]
    ] = None
    extracted: ExtractedDataType = None
