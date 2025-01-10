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

from typing import Dict, List, Optional

from pydantic import BaseModel, Field


class Entity(BaseModel):
    value: str
    merged: bool = False
    represents: List[str] = []


class EntitiesGroupSummary(BaseModel):
    title: Optional[str] = Field(default=None, description="Title of the entities group")
    color: Optional[str] = Field(
        default=None,
        description="Color of the entities group. This is for display purposes only.",
    )
    custom: bool = Field(default=False, description="Denotes if it has been created by the user")

    entities: Dict[str, Entity] = Field(
        default={},
        title="[Deprecated] Entities in the group",
        description="This field is deprecated and will be removed in future versions. It will always be empty. Use the /api/v1/kb/{kbid}/entitiesgroup/{group} endpoint to get the entities of a group.",  # noqa: E501
    )


class EntitiesGroup(BaseModel):
    title: Optional[str] = Field(default=None, description="Title of the entities group")
    color: Optional[str] = Field(
        default=None,
        description="Color of the entities group. This is for display purposes only.",
    )
    custom: bool = Field(default=False, description="Denotes if it has been created by the user")
    entities: Dict[str, Entity] = {}


class KnowledgeBoxEntities(BaseModel):
    uuid: str
    groups: Dict[str, EntitiesGroupSummary] = {}


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
