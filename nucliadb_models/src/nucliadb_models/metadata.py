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
import warnings
from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator, model_validator
from typing_extensions import Self

from nucliadb_models.utils import DateTime

from .common import Classification, FieldID, QuestionAnswer, UserClassification


class EntityRelation(BaseModel):
    entity: str
    entity_type: str


class RelationType(Enum):
    ABOUT = "ABOUT"
    CHILD = "CHILD"
    COLAB = "COLAB"
    ENTITY = "ENTITY"
    OTHER = "OTHER"
    SYNONYM = "SYNONYM"


class RelationNodeType(str, Enum):
    ENTITY = "entity"
    LABEL = "label"
    RESOURCE = "resource"
    USER = "user"


class RelationEntity(BaseModel):
    value: str
    type: RelationNodeType
    group: str | None = None

    @model_validator(mode="after")
    def check_relation_is_valid(self) -> Self:
        if self.type == RelationNodeType.ENTITY.value:
            if self.group is None:
                raise ValueError(f"All {RelationNodeType.ENTITY.value} values must define a 'group'")
        return self


class RelationMetadata(BaseModel):
    paragraph_id: str | None = None
    source_start: int | None = None
    source_end: int | None = None
    to_start: int | None = None
    to_end: int | None = None
    data_augmentation_task_id: str | None = None


class Relation(BaseModel):
    relation: RelationType
    label: str | None = None
    metadata: RelationMetadata | None = None

    from_: RelationEntity | None = Field(default=None, alias="from")
    to: RelationEntity

    @model_validator(mode="after")
    def check_relation_is_valid(self, values) -> Self:
        if self.relation == RelationType.CHILD.value:
            if self.to.type != RelationNodeType.RESOURCE.value:
                raise ValueError(
                    f"When using {RelationType.CHILD.value} relation, only "
                    f"{RelationNodeType.RESOURCE.value} entities can be used"
                )
        elif self.relation == RelationType.COLAB.value:
            if self.to.type != RelationNodeType.USER.value:
                raise ValueError(
                    f"When using {RelationType.COLAB.value} relation, only "
                    f"{RelationNodeType.USER.value} can be used"
                )
        elif self.relation == RelationType.ENTITY.value:
            if self.to.type != RelationNodeType.ENTITY.value:
                raise ValueError(
                    f"When using {RelationType.ENTITY.value} relation, only "
                    f"{RelationNodeType.ENTITY.value} can be used"
                )
        return self


class InputMetadata(BaseModel):
    metadata: dict[str, str] = {}
    language: str | None = None
    languages: list[str] | None = Field(default=None, max_length=1024)


class ResourceProcessingStatus(Enum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"
    EMPTY = "EMPTY"
    BLOCKED = "BLOCKED"
    EXPIRED = "EXPIRED"


class Metadata(InputMetadata):
    status: ResourceProcessingStatus


class FieldClassification(BaseModel):
    field: FieldID
    classifications: list[Classification] = []


class ComputedMetadata(BaseModel):
    """
    The purpose of this field is to show a cherry-picked set of fields from computed metadata
    without having to load the whole computed metadata field.
    """

    field_classifications: list[FieldClassification] = []


class UserMetadata(BaseModel):
    classifications: list[UserClassification] = []
    relations: list[Relation] = []


class TokenSplit(BaseModel):
    token: str
    klass: str
    start: int
    end: int
    cancelled_by_user: bool = False

    def __init__(self, **data):
        warnings.warn(
            f"{self.__class__.__name__} is deprecated and will be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**data)


class ParagraphAnnotation(BaseModel):
    classifications: list[UserClassification] = []
    key: str


class QuestionAnswerAnnotation(BaseModel):
    question_answer: QuestionAnswer
    cancelled_by_user: bool = False


class VisualSelection(BaseModel):
    label: str
    top: float
    left: float
    right: float
    bottom: float
    token_ids: list[int]


class PageSelections(BaseModel):
    page: int
    visual: list[VisualSelection]

    def __init__(self, **data):
        warnings.warn(
            f"{self.__class__.__name__} is deprecated and will be removed in a future version.",
            DeprecationWarning,
            stacklevel=2,
        )
        super().__init__(**data)


class UserFieldMetadata(BaseModel):
    """
    Field-level metadata set by the user via the rest api
    """

    paragraphs: list[ParagraphAnnotation] = []
    question_answers: list[QuestionAnswerAnnotation] = []
    field: FieldID


class Basic(BaseModel):
    icon: str | None = None
    title: str | None = None
    summary: str | None = None
    thumbnail: str | None = None
    created: datetime | None = None
    modified: datetime | None = None
    metadata: Metadata | None = None
    usermetadata: UserMetadata | None = None
    fieldmetadata: list[UserFieldMetadata] | None = None
    computedmetadata: ComputedMetadata | None = None
    uuid: str | None = None
    last_seqid: int | None = None
    last_account_seq: int | None = None


class InputOrigin(BaseModel):
    source_id: str | None = None
    url: str | None = None
    created: DateTime | None = Field(
        default=None,
        description="Creation date of the resource at the origin system. This can be later used for date range filtering on search endpoints. Have a look at the advanced search documentation page: https://docs.nuclia.dev/docs/rag/advanced/search/#date-filtering",
    )
    modified: DateTime | None = Field(
        default=None,
        description="Modification date of the resource at the origin system. This can be later used for date range filtering on search endpoints.  Have a look at the advanced search documentation page: https://docs.nuclia.dev/docs/rag/advanced/search/#date-filtering",
    )
    metadata: dict[str, str] = Field(
        default={},
        title="Metadata",
        description="Generic metadata from the resource at the origin system. It can later be used for filtering on search endpoints with '/origin.metadata/{key}/{value}'",
    )
    tags: list[str] = Field(
        default=[],
        title="Tags",
        description="Resource tags about the origin system. It can later be used for filtering on search endpoints with '/origin.tags/{tag}'",
        max_length=300,
    )
    collaborators: list[str] = Field(default=[], max_length=100)
    filename: str | None = None
    related: list[str] = Field(default=[], max_length=100)
    path: str | None = Field(
        default=None,
        description="Path of the original resource. Typically used to store folder structure information of the resource at the origin system. It can be later used for filtering on search endpoints with '/origin.path/{path}'",
        max_length=2048,
    )

    @field_validator("tags")
    def validate_tag_length(cls, tags):
        for tag in tags:
            if len(tag) > 512:
                raise ValueError("Each tag must be at most 1024 characters long")
        return tags


class Origin(InputOrigin):
    # Created and modified are redefined to
    # use native datetime objects and skip validation
    created: datetime | None = None
    modified: datetime | None = None

    tags: list[str] = Field(
        default=[],
        title="Tags",
        description="Resource tags about the origin system. It can later be used for filtering on search endpoints with '/origin.tags/{tag}'",
    )

    class Source(Enum):
        WEB = "WEB"
        DESKTOP = "DESKTOP"
        API = "API"
        PYSDK = "PYSDK"

    source: Source | None = Source.API


class Extra(BaseModel):
    metadata: dict[Any, Any] = Field(
        ...,
        title="Metadata",
        description="Arbitrary JSON metadata provided by the user that is not meant to be searchable, but can be serialized on results.",
    )


class Relations(BaseModel):
    relations: list[Relation] | None = None
