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

from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, model_validator
from typing_extensions import Self

from nucliadb_models.metadata import RelationNodeType


class GraphNodeMatchKind(str, Enum):
    EXACT = "exact"
    FUZZY = "fuzzy"


class GraphNode(BaseModel):
    value: Optional[str] = None
    match: GraphNodeMatchKind = GraphNodeMatchKind.EXACT
    type: Optional[RelationNodeType] = RelationNodeType.ENTITY
    group: Optional[str] = None

    @model_validator(mode="after")
    def validate_fuzzy_usage(self) -> Self:
        if self.match == GraphNodeMatchKind.FUZZY:
            if self.value is None:
                raise ValueError("Fuzzy match can only be used if a node value is provided")
            else:
                if len(self.value) < 3:
                    raise ValueError(
                        "Fuzzy match must be used with values containing at least 3 characters"
                    )

        return self


class GraphNodePosition(str, Enum):
    ANY = "any"
    SOURCE = "source"
    DESTINATION = "destination"


class PositionedGraphNode(GraphNode):
    position: GraphNodePosition = GraphNodePosition.ANY


class GraphRelation(BaseModel):
    label: Optional[str] = None


class GraphPath(BaseModel):
    source: Optional[GraphNode] = None
    relation: Optional[GraphRelation] = None
    destination: Optional[GraphNode] = None
    undirected: bool = False


class BaseGraphSearchRequest(BaseModel):
    top_k: int = Field(default=50, title="Number of results to retrieve")


class GraphSearchRequest(BaseGraphSearchRequest):
    query: GraphPath


class GraphNodesSearchRequest(BaseGraphSearchRequest):
    query: PositionedGraphNode


class GraphRelationsSearchRequest(BaseGraphSearchRequest):
    query: GraphRelation
