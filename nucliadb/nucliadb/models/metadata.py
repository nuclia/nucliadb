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

from nucliadb.models.common import FIELD_TYPES_MAP
from nucliadb_protos import resources_pb2, utils_pb2

from .common import Classification, FieldID

_T = TypeVar("_T")


class EntityRelation(BaseModel):
    entity: str
    entity_type: str


class RelationType(Enum):
    CHILD = "CHILD"
    ABOUT = "ABOUT"
    ENTITY = "ENTITY"
    COLAB = "COLAB"
    OTHER = "OTHER"


class Relation(BaseModel):
    relation: RelationType
    properties: Dict[str, str] = {}

    resource: Optional[str] = None
    label: Optional[str] = None
    user: Optional[str] = None
    other: Optional[str] = None
    entity: Optional[EntityRelation] = None

    @root_validator(pre=True)
    def check_relation_is_valid(cls, values):
        if values["relation"] == RelationType.CHILD.value:
            if "resource" not in values:
                raise ValueError(
                    "Missing 'resource' field containg the uuid of a resource"
                )
            if (
                "label" in values
                or "user" in values
                or "other" in values
                or "entity" in values
            ):
                raise ValueError(
                    "When using CHILD relation, only 'resource' field can be used"
                )

        if values["relation"] == RelationType.ABOUT.value:
            if "label" not in values:
                raise ValueError("Missing 'label' field containing a label")
            if (
                "resource" in values
                or "user" in values
                or "other" in values
                or "entity" in values
            ):
                raise ValueError(
                    "When using CHILD relation, only 'label' field can be used"
                )

        if values["relation"] == RelationType.ENTITY.value:
            if "entity" not in values:
                raise ValueError(
                    "Missing 'entity' field containing a valid entity of this resource"
                )
            if (
                "resource" in values
                or "user" in values
                or "other" in values
                or "label" in values
            ):
                raise ValueError(
                    "When using ENTITY relation, only 'entity' field can be used"
                )

        if values["relation"] == RelationType.COLAB.value:
            if "user" not in values:
                raise ValueError("Missing 'user' field containing a user reference")
            if (
                "label" in values
                or "resource" in values
                or "other" in values
                or "entity" in values
            ):
                raise ValueError(
                    "When using COLAB relation, only 'user' field can be used"
                )

        if values["relation"] == RelationType.OTHER.value:
            if "other" not in values:
                raise ValueError("Missing 'other' field")
            if (
                "label" in values
                or "resource" in values
                or "user" in values
                or "entity" in values
            ):
                raise ValueError(
                    "When using OTHER relation, only 'other' field can be used"
                )
        return values

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.Relation) -> _T:
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


def convert_pb_relation_to_api(relation: utils_pb2.Relation):
    result: Dict[str, Any] = {}
    if relation.relation == utils_pb2.Relation.RelationType.OTHER:
        result["relation"] = RelationType.OTHER.value
        result["other"] = relation.to.value
    elif relation.relation == utils_pb2.Relation.RelationType.CHILD:
        result["relation"] = RelationType.CHILD.value
        result["resource"] = relation.to.value
    elif relation.relation == utils_pb2.Relation.RelationType.ABOUT:
        result["relation"] = RelationType.ABOUT.value
        result["label"] = relation.to.value
    elif relation.relation == utils_pb2.Relation.RelationType.COLAB:
        result["relation"] = RelationType.COLAB.value
        result["user"] = relation.to.value
    elif relation.relation == utils_pb2.Relation.RelationType.ENTITY:
        result["relation"] = RelationType.ENTITY.value
        result["entity"] = EntityRelation(
            entity=relation.to.value, entity_type=relation.to.subtype
        )
    return result


class UserMetadata(BaseModel):
    classifications: List[Classification] = []
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
    uuid: Optional[str]
    last_seqid: Optional[int]


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

    source: Optional[Source]

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
