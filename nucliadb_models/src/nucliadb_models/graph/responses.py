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
from enum import Enum

from pydantic import BaseModel

from nucliadb_models.metadata import RelationNodeType, RelationType


class GraphNode(BaseModel):
    value: str
    type: RelationNodeType
    group: str


class GraphNodePosition(str, Enum):
    ANY = "any"
    SOURCE = "source"
    DESTINATION = "destination"


class PositionedGraphNode(GraphNode):
    position: GraphNodePosition = GraphNodePosition.ANY


class GraphRelation(BaseModel):
    label: str
    type: RelationType


class GraphPath(BaseModel):
    source: GraphNode
    relation: GraphRelation
    destination: GraphNode


class GraphSearchResponse(BaseModel):
    paths: list[GraphPath]


class GraphNodesSearchResponse(BaseModel):
    nodes: list[GraphNode]


class GraphRelationsSearchResponse(BaseModel):
    relations: list[GraphRelation]
