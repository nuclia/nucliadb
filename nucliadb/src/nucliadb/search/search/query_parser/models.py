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
from dataclasses import dataclass
from datetime import datetime
from typing import Literal, Optional, Union

from nidx_protos import nodereader_pb2
from pydantic import BaseModel, ConfigDict, Field

from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb_models import search as search_models
from nucliadb_models.graph.requests import GraphPathQuery
from nucliadb_protos import utils_pb2

### Retrieval

# query


class _TextQuery(BaseModel):
    query: str
    is_synonyms_query: bool
    min_score: float
    sort: search_models.SortOrder = search_models.SortOrder.DESC
    order_by: search_models.SortField = search_models.SortField.SCORE


FulltextQuery = _TextQuery
KeywordQuery = _TextQuery


class SemanticQuery(BaseModel):
    query: Optional[list[float]]
    vectorset: str
    min_score: float


class RelationQuery(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    entry_points: list[utils_pb2.RelationNode]
    # list[subtype]
    deleted_entity_groups: list[str]
    # subtype -> list[entity]
    deleted_entities: dict[str, list[str]]


class GraphQuery(BaseModel):
    query: GraphPathQuery


class Query(BaseModel):
    fulltext: Optional[FulltextQuery] = None
    keyword: Optional[KeywordQuery] = None
    semantic: Optional[SemanticQuery] = None
    relation: Optional[RelationQuery] = None
    graph: Optional[GraphQuery] = None


# filters


class Filters(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    field_expression: Optional[nodereader_pb2.FilterExpression] = None
    paragraph_expression: Optional[nodereader_pb2.FilterExpression] = None
    filter_expression_operator: nodereader_pb2.FilterOperator.ValueType = (
        nodereader_pb2.FilterOperator.AND
    )

    autofilter: Optional[list[utils_pb2.RelationNode]] = None
    facets: list[str] = Field(default_factory=list)
    hidden: Optional[bool] = None
    security: Optional[search_models.RequestSecurity] = None
    with_duplicates: bool = False


class DateTimeFilter(BaseModel):
    after: Optional[datetime] = None  # aka, start
    before: Optional[datetime] = None  # aka, end


# rank fusion


class RankFusion(BaseModel):
    window: int = Field(le=500)


class ReciprocalRankFusion(RankFusion):
    k: float = Field(default=60.0)
    boosting: search_models.ReciprocalRankFusionWeights = Field(
        default_factory=search_models.ReciprocalRankFusionWeights
    )


# reranking


class NoopReranker(BaseModel):
    pass


class PredictReranker(BaseModel):
    window: int = Field(le=200)


Reranker = Union[NoopReranker, PredictReranker]

# retrieval and generation operations


class UnitRetrieval(BaseModel):
    query: Query
    top_k: int
    filters: Filters = Field(default_factory=Filters)
    rank_fusion: Optional[RankFusion] = None
    reranker: Optional[Reranker] = None


# TODO: augmentation things: hydration...


class Generation(BaseModel):
    use_visual_llm: bool
    max_context_tokens: int
    max_answer_tokens: Optional[int]


class ParsedQuery(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)

    fetcher: Fetcher
    retrieval: UnitRetrieval
    generation: Optional[Generation] = None


### Catalog
@dataclass
class CatalogExpression:
    @dataclass
    class Date:
        field: Union[Literal["created_at"], Literal["modified_at"]]
        since: Optional[datetime]
        until: Optional[datetime]

    bool_and: Optional[list["CatalogExpression"]] = None
    bool_or: Optional[list["CatalogExpression"]] = None
    bool_not: Optional["CatalogExpression"] = None
    date: Optional[Date] = None
    facet: Optional[str] = None
    resource_id: Optional[str] = None


class CatalogQuery(BaseModel):
    kbid: str
    query: Optional[search_models.CatalogQuery]
    filters: Optional[CatalogExpression]
    sort: search_models.SortOptions
    faceted: list[str]
    page_size: int
    page_number: int


### Graph


# Right now, we don't need a more generic model for graph queries, we can
# directly use the protobuffer directly
GraphRetrieval = nodereader_pb2.GraphSearchRequest


# We need this to avoid issues with pydantic and generic types defined in another module
GraphQuery.model_rebuild()
