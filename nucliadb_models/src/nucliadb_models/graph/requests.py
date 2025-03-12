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

## Models for graph nodes and relations


class NodeMatchKind(str, Enum):
    EXACT = "exact"
    FUZZY = "fuzzy"


class GraphNode(BaseModel):
    value: Optional[str] = None
    match: NodeMatchKind = NodeMatchKind.EXACT
    type: Optional[RelationNodeType] = RelationNodeType.ENTITY
    group: Optional[str] = None

    @model_validator(mode="after")
    def validate_fuzzy_usage(self) -> Self:
        if self.match == NodeMatchKind.FUZZY:
            if self.value is None:
                raise ValueError("Fuzzy match can only be used if a node value is provided")
            else:
                if len(self.value) < 3:
                    raise ValueError(
                        "Fuzzy match must be used with values containing at least 3 characters"
                    )
        return self


class GraphRelation(BaseModel):
    label: Optional[str] = None


## Models for query expressions


class AnyNode(GraphNode):
    prop: Literal["node"]


class SourceNode(GraphNode):
    prop: Literal["source_node"]


class DestinationNode(GraphNode):
    prop: Literal["destination_node"]


class Relation(GraphRelation):
    prop: Literal["relation"]


class GraphPath(BaseModel, extra="forbid"):
    prop: Literal["path"] = "path"
    source: Optional[GraphNode] = None
    relation: Optional[GraphRelation] = None
    destination: Optional[GraphNode] = None
    undirected: bool = False


## Requests models


class BaseGraphSearchRequest(BaseModel):
    top_k: int = Field(default=50, title="Number of results to retrieve")


graph_query_discriminator = filter_discriminator


# Paths search

GraphPathQuery = Annotated[
    Union[
        # bool expressions
        Annotated[And["GraphPathQuery"], Tag("and")],
        Annotated[Or["GraphPathQuery"], Tag("or")],
        Annotated[Not["GraphPathQuery"], Tag("not")],
        # paths
        Annotated[GraphPath, Tag("path")],
        # nodes
        Annotated[SourceNode, Tag("source_node")],
        Annotated[DestinationNode, Tag("destination_node")],
        Annotated[AnyNode, Tag("node")],
        # relations
        Annotated[Relation, Tag("relation")],
    ],
    Discriminator(graph_query_discriminator),
]


class GraphSearchRequest(BaseGraphSearchRequest):
    query: GraphPathQuery


# Nodes search

GraphNodesQuery = Annotated[
    Union[
        Annotated[And["GraphNodesQuery"], Tag("and")],
        Annotated[Or["GraphNodesQuery"], Tag("or")],
        Annotated[Not["GraphNodesQuery"], Tag("not")],
        Annotated[AnyNode, Tag("node")],
    ],
    Discriminator(graph_query_discriminator),
]


class GraphNodesSearchRequest(BaseGraphSearchRequest):
    query: GraphNodesQuery


# Relations search

GraphRelationsQuery = Annotated[
    Union[
        Annotated[Or["GraphRelationsQuery"], Tag("or")],
        Annotated[Not["GraphRelationsQuery"], Tag("not")],
        Annotated[Relation, Tag("relation")],
    ],
    Discriminator(graph_query_discriminator),
]


class GraphRelationsSearchRequest(BaseGraphSearchRequest):
    query: GraphRelationsQuery


# We need this to avoid issues with pydantic and generic types defined in another module
GraphSearchRequest.model_rebuild()
GraphNodesSearchRequest.model_rebuild()
GraphRelationsSearchRequest.model_rebuild()
