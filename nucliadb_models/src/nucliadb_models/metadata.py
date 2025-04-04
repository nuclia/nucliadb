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
import warnings
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, model_validator
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
    group: Optional[str] = None

    @model_validator(mode="after")
    def check_relation_is_valid(self) -> Self:
        if self.type == RelationNodeType.ENTITY.value:
            if self.group is None:
                raise ValueError(f"All {RelationNodeType.ENTITY.value} values must define a 'group'")
        return self


class RelationMetadata(BaseModel):
    paragraph_id: Optional[str] = None
    source_start: Optional[int] = None
    source_end: Optional[int] = None
    to_start: Optional[int] = None
    to_end: Optional[int] = None
    data_augmentation_task_id: Optional[str] = None


class Relation(BaseModel):
    relation: RelationType
    label: Optional[str] = None
    metadata: Optional[RelationMetadata] = None

    from_: Optional[RelationEntity] = Field(None, alias="from")
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
    metadata: Dict[str, str] = {}
    language: Optional[str] = None
    languages: Optional[List[str]] = Field(default=None, max_length=1024)


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
    classifications: List[Classification] = []


class ComputedMetadata(BaseModel):
    """
    The purpose of this field is to show a cherry-picked set of fields from computed metadata
    without having to load the whole computed metadata field.
    """

    field_classifications: List[FieldClassification] = []


class UserMetadata(BaseModel):
    classifications: List[UserClassification] = []
    relations: List[Relation] = []


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
    classifications: List[UserClassification] = []
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
    token_ids: List[int]


class PageSelections(BaseModel):
    page: int
    visual: List[VisualSelection]

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

    paragraphs: List[ParagraphAnnotation] = []
    question_answers: List[QuestionAnswerAnnotation] = []
    field: FieldID


class Basic(BaseModel):
    icon: Optional[str] = None
    title: Optional[str] = None
    summary: Optional[str] = None
    thumbnail: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    metadata: Optional[Metadata] = None
    usermetadata: Optional[UserMetadata] = None
    fieldmetadata: Optional[List[UserFieldMetadata]] = None
    computedmetadata: Optional[ComputedMetadata] = None
    uuid: Optional[str] = None
    last_seqid: Optional[int] = None
    last_account_seq: Optional[int] = None


class InputOrigin(BaseModel):
    source_id: Optional[str] = None
    url: Optional[str] = None
    created: Optional[DateTime] = Field(
        default=None,
        description="Creation date of the resource at the origin system. This can be later used for date range filtering on search endpoints. Have a look at the advanced search documentation page: https://docs.nuclia.dev/docs/rag/advanced/search/#date-filtering",
    )
    modified: Optional[DateTime] = Field(
        default=None,
        description="Modification date of the resource at the origin system. This can be later used for date range filtering on search endpoints.  Have a look at the advanced search documentation page: https://docs.nuclia.dev/docs/rag/advanced/search/#date-filtering",
    )
    metadata: Dict[str, str] = Field(
        default={},
        title="Metadata",
        description="Generic metadata from the resource at the origin system. It can later be used for filtering on search endpoints with '/origin.metadata/{key}/{value}'",
    )
    tags: List[str] = Field(
        default=[],
        title="Tags",
        description="Resource tags about the origin system. It can later be used for filtering on search endpoints with '/origin.tags/{tag}'",
    )
    collaborators: List[str] = []
    filename: Optional[str] = None
    related: List[str] = []
    path: Optional[str] = Field(
        default=None,
        description="Path of the original resource. Typically used to store folder structure information of the resource at the origin system. It can be later used for filtering on search endpoints with '/origin.path/{path}'",
    )


class Origin(InputOrigin):
    # Created and modified are redefined to
    # use native datetime objects and skip validation
    created: Optional[datetime] = None
    modified: Optional[datetime] = None

    class Source(Enum):
        WEB = "WEB"
        DESKTOP = "DESKTOP"
        API = "API"
        PYSDK = "PYSDK"

    source: Optional[Source] = Source.API


class Extra(BaseModel):
    metadata: Dict[Any, Any] = Field(
        ...,
        title="Metadata",
        description="Arbitrary JSON metadata provided by the user that is not meant to be searchable, but can be serialized on results.",  # noqa
    )


class Relations(BaseModel):
    relations: Optional[List[Relation]] = None
