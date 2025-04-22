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

from pydantic import (
    BaseModel,
    Field,
)

from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb_models import search as search_models
from nucliadb_protos import nodereader_pb2, utils_pb2

### Retrieval

# query


@dataclass
class _TextQuery:
    query: str
    is_synonyms_query: bool
    min_score: float
    sort: search_models.SortOrder = search_models.SortOrder.DESC
    order_by: search_models.SortField = search_models.SortField.SCORE


FulltextQuery = _TextQuery
KeywordQuery = _TextQuery


@dataclass
class SemanticQuery:
    query: Optional[list[float]]
    vectorset: str
    min_score: float


@dataclass
class RelationQuery:
    detected_entities: list[utils_pb2.RelationNode]
    # list[subtype]
    deleted_entity_groups: list[str]
    # subtype -> list[entity]
    deleted_entities: dict[str, list[str]]


@dataclass
class Query:
    fulltext: Optional[FulltextQuery] = None
    keyword: Optional[KeywordQuery] = None
    semantic: Optional[SemanticQuery] = None
    relation: Optional[RelationQuery] = None


# filters


@dataclass
class Filters:
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


@dataclass
class UnitRetrieval:
    query: Query
    top_k: int
    filters: Filters
    # TODO: rank fusion depends on the response building, not the retrieval
    rank_fusion: RankFusion
    # TODO: reranking fusion depends on the response building, not the retrieval
    reranker: Reranker


@dataclass
class Generation:
    use_visual_llm: bool
    max_context_tokens: int
    max_answer_tokens: Optional[int]


@dataclass
class ParsedQuery:
    fetcher: Fetcher
    retrieval: UnitRetrieval
    generation: Optional[Generation] = None
    # TODO: add merge, rank fusion, rerank...


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
    query: str
    filters: Optional[CatalogExpression]
    sort: search_models.SortOptions
    faceted: list[str]
    page_size: int
    page_number: int


### Graph


# Right now, we don't need a more generic model for graph queries, we can
# directly use the protobuffer directly
GraphRetrieval = nodereader_pb2.GraphSearchRequest
