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
from pydantic import BaseModel
from pydantic.class_validators import root_validator

from nucliadb_models.common import FIELD_TYPES_MAP
from nucliadb_protos import resources_pb2, utils_pb2

from .common import Classification, FieldID, UserClassification

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


RelationNodeTypeMap: Dict[
    RelationNodeType, utils_pb2.RelationNode.NodeType.ValueType
] = {
    RelationNodeType.ENTITY: utils_pb2.RelationNode.NodeType.ENTITY,
    RelationNodeType.LABEL: utils_pb2.RelationNode.NodeType.LABEL,
    RelationNodeType.RESOURCE: utils_pb2.RelationNode.NodeType.RESOURCE,
    RelationNodeType.USER: utils_pb2.RelationNode.NodeType.USER,
}

RelationNodeTypePbMap: Dict[
    utils_pb2.RelationNode.NodeType.ValueType, RelationNodeType
] = {
    utils_pb2.RelationNode.NodeType.ENTITY: RelationNodeType.ENTITY,
    utils_pb2.RelationNode.NodeType.LABEL: RelationNodeType.LABEL,
    utils_pb2.RelationNode.NodeType.RESOURCE: RelationNodeType.RESOURCE,
    utils_pb2.RelationNode.NodeType.USER: RelationNodeType.USER,
}


class RelationEntity(BaseModel):
    value: str
    type: RelationNodeType
    group: Optional[str] = None

    @root_validator(pre=True)
    def check_relation_is_valid(cls, values):
        if values["type"] == RelationNodeType.ENTITY.value:
            if "group" not in values:
                raise ValueError(
                    f"All {RelationNodeType.ENTITY.value} values must define a 'group'"
                )
        return values


class Relation(BaseModel):
    from_: Optional[RelationEntity]
    to: RelationEntity
    relation: RelationType

    class Config:
        fields = {"from_": "from"}

    @root_validator(pre=False)
    def check_relation_is_valid(cls, values):
        if values["relation"] == RelationType.CHILD.value:
            if values["to"].get("type") != RelationNodeType.RESOURCE.value:
                raise ValueError(
                    f"When using {RelationType.CHILD.value} relation, only "
                    f"{RelationNodeType.RESOURCE.value} entities can be used"
                )
        elif values["relation"] == RelationType.COLAB.value:
            if values["to"].get("type") != RelationNodeType.USER.value:
                raise ValueError(
                    f"When using {RelationType.COLAB.value} relation, only "
                    f"{RelationNodeType.USER.value} can be used"
                )
        elif values["relation"] == RelationType.ENTITY.value:
            if values["to"].get("type") != RelationNodeType.ENTITY.value:
                raise ValueError(
                    f"When using {RelationType.ENTITY.value} relation, only "
                    f"{RelationNodeType.ENTITY.value} can be used"
                )
        return values

    @classmethod
    def from_message(cls: Type[_T], message: utils_pb2.Relation) -> _T:
        value = convert_pb_relation_to_api(message)
        return cls(**value)


class InputMetadata(BaseModel):
    metadata: Dict[str, str] = {}
    language: Optional[str]
    languages: Optional[List[str]]


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
                        field_type=FIELD_TYPES_MAP[fc.field.field_type],
                    ),
                    classifications=[
                        Classification(label=c.label, labelset=c.labelset)
                        for c in fc.classifications
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
        value["relations"] = [
            convert_pb_relation_to_api(relation) for relation in message.relations
        ]
        return cls(**value)


class TokenSplit(BaseModel):
    token: str
    klass: str
    start: int
    end: int


class ParagraphAnnotation(BaseModel):
    classifications: List[Classification] = []
    key: str


class UserFieldMetadata(BaseModel):
    """
    Field-level metadata set by the user via the rest api
    """

    token: List[TokenSplit] = []
    paragraphs: List[ParagraphAnnotation] = []
    field: FieldID

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.UserFieldMetadata) -> _T:
        value = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
            use_integers_for_enums=True,
        )
        value["field"]["field_type"] = FIELD_TYPES_MAP[value["field"]["field_type"]]
        return cls(**value)


class Basic(BaseModel):
    icon: Optional[str]
    title: Optional[str]
    summary: Optional[str]
    thumbnail: Optional[str]
    layout: Optional[str]
    created: Optional[datetime]
    modified: Optional[datetime]
    metadata: Optional[Metadata]
    usermetadata: Optional[UserMetadata]
    fieldmetadata: Optional[List[UserFieldMetadata]]
    computedmetadata: Optional[ComputedMetadata]
    uuid: Optional[str]
    last_seqid: Optional[int]
    last_account_seq: Optional[int]


class InputOrigin(BaseModel):
    source_id: Optional[str] = None
    url: Optional[str] = None
    created: Optional[datetime] = None
    modified: Optional[datetime] = None
    metadata: Dict[str, str] = {}
    tags: List[str] = []
    colaborators: List[str] = []
    filename: Optional[str] = None
    related: List[str] = []


class Origin(InputOrigin):
    class Source(Enum):
        WEB = "WEB"
        DESKTOP = "DESKTOP"
        API = "API"
        PYSDK = "PYSDK"

    source: Optional[Source] = Source.API

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Origin) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )


class Relations(BaseModel):
    relations: Optional[List[Relation]]
