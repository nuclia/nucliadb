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
from typing import Literal, Optional, Union

from pydantic import BaseModel, Field

from nucliadb_models.filters import FilterExpression
from nucliadb_models.graph.requests import GraphPathQuery
from nucliadb_models.search import RankFusion, RankFusionName, SearchParamDefaults
from nucliadb_models.security import RequestSecurity


class KeywordQuery(BaseModel):
    query: str
    min_score: float


class SemanticQuery(BaseModel):
    query: list[float]
    vectorset: str
    min_score: float


class GraphQuery(BaseModel):
    query: GraphPathQuery


class Query(BaseModel):
    keyword: Optional[KeywordQuery] = None
    semantic: Optional[SemanticQuery] = None
    graph: Optional[GraphQuery] = None


class Filters(BaseModel):
    filter_expression: Optional[FilterExpression] = (
        SearchParamDefaults.filter_expression.to_pydantic_field()
    )
    show_hidden: bool = SearchParamDefaults.show_hidden.to_pydantic_field()
    security: Optional[RequestSecurity] = None
    with_duplicates: bool = False


class RetrievalRequest(BaseModel):
    query: Query
    top_k: int = Field(default=20, gt=0, le=500)
    filters: Filters = Field(default_factory=Filters)
    rank_fusion: Union[RankFusionName, RankFusion] = Field(default=RankFusionName.RECIPROCAL_RANK_FUSION)


class ScoreSource(str, Enum):
    INDEX = "index"
    RANK_FUSION = "rank_fusion"
    RERANKER = "reranker"


class ScoreType(str, Enum):
    SEMANTIC = "semantic"
    KEYWORD = "keyword"
    GRAPH = "graph"
    RRF = "rrf"
    WCOMB_SUM = "wCombSUM"
    DEFAULT_RERANKER = "default_reranker"


class KeywordScore(BaseModel):
    score: float
    source: Literal[ScoreSource.INDEX] = ScoreSource.INDEX
    type: Literal[ScoreType.KEYWORD] = ScoreType.KEYWORD


class SemanticScore(BaseModel):
    score: float
    source: Literal[ScoreSource.INDEX] = ScoreSource.INDEX
    type: Literal[ScoreType.SEMANTIC] = ScoreType.SEMANTIC


class GraphScore(BaseModel):
    score: float
    source: Literal[ScoreSource.INDEX] = ScoreSource.INDEX
    type: Literal[ScoreType.GRAPH] = ScoreType.GRAPH


class RrfScore(BaseModel):
    score: float
    source: Literal[ScoreSource.RANK_FUSION] = ScoreSource.RANK_FUSION
    type: Literal[ScoreType.RRF] = ScoreType.RRF


class WeightedCombSumScore(BaseModel):
    score: float
    source: Literal[ScoreSource.RANK_FUSION] = ScoreSource.RANK_FUSION
    type: Literal[ScoreType.WCOMB_SUM] = ScoreType.WCOMB_SUM


class RerankerScore(BaseModel):
    score: float
    source: Literal[ScoreSource.RERANKER] = ScoreSource.RERANKER
    type: Literal[ScoreType.DEFAULT_RERANKER] = ScoreType.DEFAULT_RERANKER


Score = KeywordScore | SemanticScore | GraphScore | RrfScore | WeightedCombSumScore | RerankerScore


class Scores(BaseModel):
    value: float
    source: ScoreSource
    type: ScoreType
    history: list[Score]


class Metadata(BaseModel):
    field_labels: list[str]
    paragraph_labels: list[str]

    is_an_image: bool
    is_a_table: bool

    # for extracted from visual content (ocr, inception, tables)
    source_file: str | None

    # for documents (pdf, docx...) only
    page: int | None
    in_page_with_visual: bool | None


class RetrievalMatch(BaseModel):
    id: str
    score: Scores
    metadata: Metadata


class RetrievalResponse(BaseModel):
    matches: list[RetrievalMatch]
