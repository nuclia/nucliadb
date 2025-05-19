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
