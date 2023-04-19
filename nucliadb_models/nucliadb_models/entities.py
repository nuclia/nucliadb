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

from typing import Dict, List, Optional, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field

from nucliadb_protos import knowledgebox_pb2

_T = TypeVar("_T")


class Entity(BaseModel):
    value: str
    merged: bool = False
    represents: List[str] = []

    @classmethod
    def from_message(cls: Type[_T], message: knowledgebox_pb2.Entity) -> _T:
        entity = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        return cls(**entity)


class EntitiesGroup(BaseModel):
    entities: Dict[str, Entity] = {}
    title: Optional[str] = None
    color: Optional[str] = None

    custom: bool = Field(
        default=False, description="Denotes if it has been created by the user"
    )

    @classmethod
    def from_message(
        cls: Type[_T],
        message: knowledgebox_pb2.EntitiesGroup,
    ) -> _T:
        entities_group = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        entities_group["entities"] = {}

        for name, entity in message.entities.items():
            if not entity.deleted:
                entities_group["entities"][name] = Entity.from_message(entity)

        return cls(**entities_group)

    @classmethod
    def from_summary_message(
        cls: Type[_T],
        message: knowledgebox_pb2.EntitiesGroupSummary,
    ) -> _T:
        entitiesgroup = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        return cls(**entitiesgroup)


class KnowledgeBoxEntities(BaseModel):
    uuid: str
    groups: Dict[str, EntitiesGroup] = {}


class CreateEntitiesGroupPayload(BaseModel):
    group: str
    entities: Dict[str, Entity] = {}
    title: Optional[str] = None
    color: Optional[str] = None


class UpdateEntitiesGroupPayload(BaseModel):
    title: Optional[str] = None
    color: Optional[str] = None

    add: Dict[str, Entity] = {}
    update: Dict[str, Entity] = {}
    delete: List[str] = []
