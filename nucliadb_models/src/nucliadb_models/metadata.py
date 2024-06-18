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
from typing import Any, Dict, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self

from nucliadb_models.common import FIELD_TYPES_MAP
from nucliadb_protos import resources_pb2, utils_pb2

from .common import Classification, FieldID, QuestionAnswer, UserClassification

_T = TypeVar("_T")


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


RelationTypePbMap: Dict[utils_pb2.Relation.RelationType.ValueType, RelationType] = {
    utils_pb2.Relation.RelationType.ABOUT: RelationType.ABOUT,
    utils_pb2.Relation.RelationType.CHILD: RelationType.CHILD,
    utils_pb2.Relation.RelationType.COLAB: RelationType.COLAB,
    utils_pb2.Relation.RelationType.ENTITY: RelationType.ENTITY,
    utils_pb2.Relation.RelationType.OTHER: RelationType.OTHER,
    utils_pb2.Relation.RelationType.SYNONYM: RelationType.SYNONYM,
}

RelationTypeMap: Dict[RelationType, utils_pb2.Relation.RelationType.ValueType] = {
    RelationType.ABOUT: utils_pb2.Relation.RelationType.ABOUT,
    RelationType.CHILD: utils_pb2.Relation.RelationType.CHILD,
    RelationType.COLAB: utils_pb2.Relation.RelationType.COLAB,
    RelationType.ENTITY: utils_pb2.Relation.RelationType.ENTITY,
    RelationType.OTHER: utils_pb2.Relation.RelationType.OTHER,
    RelationType.SYNONYM: utils_pb2.Relation.RelationType.SYNONYM,
}


class RelationNodeType(str, Enum):
    ENTITY = "entity"
    LABEL = "label"
    RESOURCE = "resource"
    USER = "user"


RelationNodeTypeMap: Dict[RelationNodeType, utils_pb2.RelationNode.NodeType.ValueType] = {
    RelationNodeType.ENTITY: utils_pb2.RelationNode.NodeType.ENTITY,
    RelationNodeType.LABEL: utils_pb2.RelationNode.NodeType.LABEL,
    RelationNodeType.RESOURCE: utils_pb2.RelationNode.NodeType.RESOURCE,
    RelationNodeType.USER: utils_pb2.RelationNode.NodeType.USER,
}

RelationNodeTypePbMap: Dict[utils_pb2.RelationNode.NodeType.ValueType, RelationNodeType] = {
    utils_pb2.RelationNode.NodeType.ENTITY: RelationNodeType.ENTITY,
    utils_pb2.RelationNode.NodeType.LABEL: RelationNodeType.LABEL,
    utils_pb2.RelationNode.NodeType.RESOURCE: RelationNodeType.RESOURCE,
    utils_pb2.RelationNode.NodeType.USER: RelationNodeType.USER,
}


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

    @classmethod
    def from_message(cls: Type[_T], message: utils_pb2.RelationMetadata) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


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

    @classmethod
    def from_message(cls: Type[_T], message: utils_pb2.Relation) -> _T:
        value = convert_pb_relation_to_api(message)
        return cls(**value)


class InputMetadata(BaseModel):
    metadata: Dict[str, str] = {}
    language: Optional[str] = None
    languages: Optional[List[str]] = None


class ResourceProcessingStatus(Enum):
    PENDING = "PENDING"
    PROCESSED = "PROCESSED"
    ERROR = "ERROR"
    EMPTY = "EMPTY"
    BLOCKED = "BLOCKED"
    EXPIRED = "EXPIRED"


class Metadata(InputMetadata):
    status: ResourceProcessingStatus

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Metadata) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


def convert_pb_relation_node_to_api(
    relation_node: utils_pb2.RelationNode,
) -> Dict[str, Any]:
    return {
        "type": RelationNodeTypePbMap[relation_node.ntype],
        "value": relation_node.value,
        "group": relation_node.subtype,
    }


def convert_pb_relation_to_api(relation: utils_pb2.Relation) -> Dict[str, Any]:
    return {
        "relation": RelationTypePbMap[relation.relation],
        "from": convert_pb_relation_node_to_api(relation.source),
        "to": convert_pb_relation_node_to_api(relation.to),
        "label": relation.relation_label,
        "metadata": RelationMetadata.from_message(relation.metadata),
    }


class FieldClassification(BaseModel):
    field: FieldID
    classifications: List[Classification] = []


class ComputedMetadata(BaseModel):
    """
    The purpose of this field is to show a cherry-picked set of fields from computed metadata
    without having to load the whole computed metadata field.
    """

    field_classifications: List[FieldClassification] = []

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.ComputedMetadata) -> _T:
        values: Dict[str, List[FieldClassification]] = {"field_classifications": []}
        for fc in message.field_classifications:
            values["field_classifications"].append(
                FieldClassification(
                    field=FieldID(
                        field=fc.field.field,
                        field_type=FIELD_TYPES_MAP[fc.field.field_type].value,  # type: ignore
                    ),
                    classifications=[
                        Classification(label=c.label, labelset=c.labelset) for c in fc.classifications
                    ],
                )
            )
        return cls(**values)


class UserMetadata(BaseModel):
    classifications: List[UserClassification] = []
    relations: List[Relation] = []

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.UserMetadata) -> _T:
        value = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        value["relations"] = [convert_pb_relation_to_api(relation) for relation in message.relations]
        return cls(**value)


class TokenSplit(BaseModel):
    token: str
    klass: str
    start: int
    end: int
    cancelled_by_user: bool = False


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


class UserFieldMetadata(BaseModel):
    """
    Field-level metadata set by the user via the rest api
    """

    token: List[TokenSplit] = []
    paragraphs: List[ParagraphAnnotation] = []
    selections: List[PageSelections] = []
    question_answers: List[QuestionAnswerAnnotation] = []
    field: FieldID

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.UserFieldMetadata) -> _T:
        value = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
            use_integers_for_enums=True,
        )
        value["selections"] = [
            MessageToDict(
                selections,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
                use_integers_for_enums=True,
            )
            for selections in message.page_selections
        ]

        value["field"]["field_type"] = FIELD_TYPES_MAP[value["field"]["field_type"]].value
        return cls(**value)


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
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    metadata: Dict[str, str] = {}
    tags: List[str] = []
    collaborators: List[str] = []
    # old field was "colaborators"
    filename: Optional[str] = None
    related: List[str] = []
    path: Optional[str] = None


class Origin(InputOrigin):
    class Source(Enum):
        WEB = "WEB"
        DESKTOP = "DESKTOP"
        API = "API"
        PYSDK = "PYSDK"

    source: Optional[Source] = Source.API

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Origin) -> _T:
        data = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        # old field was "colaborators" and we want to keep pb field name
        # to avoid migration
        data["collaborators"] = data.pop("colaborators", [])
        return cls(**data)


class Extra(BaseModel):
    metadata: Dict[Any, Any] = Field(
        ...,
        title="Metadata",
        description="Arbitrary JSON metadata provided by the user that is not meant to be searchable, but can be serialized on results.",  # noqa
    )

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Extra) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=False,
            )
        )


class Relations(BaseModel):
    relations: Optional[List[Relation]] = None
