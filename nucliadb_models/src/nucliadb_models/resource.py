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

import string
from datetime import datetime
from enum import Enum
from typing import Any

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


class KnowledgeBoxConfig(BaseModel):
    slug: SlugString | None = Field(
        default=None, title="Slug", description="Slug for the Knowledge Box."
    )
    title: str | None = Field(default=None, title="Title", description="Title for the Knowledge Box.")
    description: str | None = Field(
        default=None,
        title="Description",
        description="Description for the Knowledge Box.",
    )
    learning_configuration: dict[str, Any] | None = Field(
        default=None,
        title="Learning Configuration",
        description="Learning configuration for the Knowledge Box. If provided, NucliaDB will set the learning configuration for the Knowledge Box.",
    )

    external_index_provider: ExternalIndexProvider | None = Field(
        default=None,
        title="External Index Provider",
        description="External index provider for the Knowledge Box.",
    )

    configured_external_index_provider: dict[str, Any] | None = Field(
        default=None,
        title="Configured External Index Provider",
        description="Metadata for the configured external index provider (if any)",
    )

    similarity: VectorSimilarity | None = Field(
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
    def id_check(cls, v: str | None) -> str | None:
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
    slug: SlugString | None = None
    uuid: str


class KnowledgeBoxObjID(BaseModel):
    uuid: str


class KnowledgeBoxObj(BaseModel):
    """
    The API representation of a Knowledge Box object.
    """

    slug: SlugString | None = None
    uuid: str
    config: KnowledgeBoxConfig | None = None
    model: SemanticModelMetadata | None = None


class KnowledgeBoxList(BaseModel):
    kbs: list[KnowledgeBoxObjSummary] = []


# Resources


class ExtractedData(BaseModel):
    text: ExtractedText | None = None
    metadata: FieldComputedMetadata | None = None
    large_metadata: LargeComputedMetadata | None = None
    vectors: VectorObject | None = None
    question_answers: FieldQuestionAnswers | None = None


class TextFieldExtractedData(ExtractedData):
    pass


class FileFieldExtractedData(ExtractedData):
    file: FileExtractedData | None = None


class LinkFieldExtractedData(ExtractedData):
    link: LinkExtractedData | None = None


class ConversationFieldExtractedData(ExtractedData):
    pass


ExtractedDataType = (
    TextFieldExtractedData
    | FileFieldExtractedData
    | LinkFieldExtractedData
    | ConversationFieldExtractedData
    | None
)


class Error(BaseModel):
    body: str
    code: int
    code_str: str
    created: datetime | None
    severity: str


class FieldData(BaseModel): ...


class TextFieldData(BaseModel):
    value: FieldText | None = None
    extracted: TextFieldExtractedData | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


class FileFieldData(BaseModel):
    value: FieldFile | None = None
    extracted: FileFieldExtractedData | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


class LinkFieldData(BaseModel):
    value: FieldLink | None = None
    extracted: LinkFieldExtractedData | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


class ConversationFieldData(BaseModel):
    value: FieldConversation | None = None
    extracted: ConversationFieldExtractedData | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


class GenericFieldData(BaseModel):
    value: str | None = None
    extracted: TextFieldExtractedData | None = None
    error: Error | None = None
    status: str | None = None
    errors: list[Error] | None = None


class ResourceData(BaseModel):
    texts: dict[str, TextFieldData] | None = None
    files: dict[str, FileFieldData] | None = None
    links: dict[str, LinkFieldData] | None = None
    conversations: dict[str, ConversationFieldData] | None = None
    generics: dict[str, GenericFieldData] | None = None


class QueueType(str, Enum):
    PRIVATE = "private"
    SHARED = "shared"


class Resource(BaseModel):
    id: str

    # This first block of attributes correspond to Basic fields
    slug: str | None = None
    title: str | None = None
    summary: str | None = None
    icon: str | None = None
    thumbnail: str | None = None
    metadata: Metadata | None = None
    usermetadata: UserMetadata | None = None
    fieldmetadata: list[UserFieldMetadata] | None = None
    computedmetadata: ComputedMetadata | None = None
    created: datetime | None = None
    modified: datetime | None = None
    last_seqid: int | None = None
    last_account_seq: int | None = None
    queue: QueueType | None = None
    hidden: bool | None = None

    origin: Origin | None = None
    extra: Extra | None = None
    relations: list[Relation] | None = None

    data: ResourceData | None = None

    security: ResourceSecurity | None = Field(
        default=None,
        title="Security",
        description="Resource security metadata",
    )


class ResourcePagination(BaseModel):
    page: int
    size: int
    last: bool


class ResourceList(BaseModel):
    resources: list[Resource]
    pagination: ResourcePagination


class ResourceField(BaseModel):
    field_type: FieldTypeName
    field_id: str
    value: FieldText | FieldFile | FieldLink | Conversation | None = None
    extracted: ExtractedDataType = None
