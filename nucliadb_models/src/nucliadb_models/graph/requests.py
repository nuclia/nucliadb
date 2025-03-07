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
from typing import Annotated, Literal, Optional, Union

from pydantic import BaseModel, Discriminator, Field, Tag, model_validator
from typing_extensions import Self

from nucliadb_models.filters import And, Not, Or, filter_discriminator
from nucliadb_models.metadata import RelationNodeType

# FIXME For some reason, pydantic breaks if we try to reuse the same And/Or/Not
# classes from the filters module but redefining them as subclasses works


class And(And):  # type: ignore[no-redef]
    ...


class Or(Or):  # type: ignore[no-redef]
    ...


class Not(Not):  # type: ignore[no-redef]
    ...


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


class GraphPath(BaseModel, extra="forbid"):
    prop: Literal["path"]
    source: Optional[GraphNode] = None
    relation: Optional[GraphRelation] = None
    destination: Optional[GraphNode] = None
    undirected: bool = False


class BaseGraphSearchRequest(BaseModel):
    top_k: int = Field(default=50, title="Number of results to retrieve")


graph_query_discriminator = filter_discriminator


GraphPathQuery = Annotated[
    Union[
        Annotated[And["GraphPathQuery"], Tag("and")],
        Annotated[Or["GraphPathQuery"], Tag("or")],
        Annotated[Not["GraphPathQuery"], Tag("not")],
        Annotated[GraphPath, Tag("path")],
    ],
    Discriminator(graph_query_discriminator),
]


class GraphSearchRequest(BaseGraphSearchRequest):
    query: GraphPathQuery


class GraphNodesSearchRequest(BaseGraphSearchRequest):
    query: PositionedGraphNode


class GraphRelationsSearchRequest(BaseGraphSearchRequest):
    query: GraphRelation
