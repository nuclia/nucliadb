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
from typing import Literal

from pydantic import BaseModel, Field

from nucliadb_models.filters import FilterExpression
from nucliadb_models.graph.requests import GraphPathQuery
from nucliadb_models.search import RankFusion, RankFusionName, SearchParamDefaults
from nucliadb_models.security import RequestSecurity


class KeywordQuery(BaseModel):
    query: str
    min_score: float = 0.0
    with_synonyms: bool = False


class SemanticQuery(BaseModel):
    query: list[float]
    vectorset: str
    min_score: float = -1.0


class GraphQuery(BaseModel):
    query: GraphPathQuery


class Query(BaseModel):
    keyword: KeywordQuery | None = None
    semantic: SemanticQuery | None = None
    graph: GraphQuery | None = None


class Filters(BaseModel):
    filter_expression: FilterExpression | None = (
        SearchParamDefaults.filter_expression.to_pydantic_field()
    )
    show_hidden: bool = SearchParamDefaults.show_hidden.to_pydantic_field()
    security: RequestSecurity | None = None
    with_duplicates: bool = False


class RetrievalRequest(BaseModel):
    query: Query
    top_k: int = Field(default=20, gt=0, le=500)
    filters: Filters = Field(default_factory=Filters)
    rank_fusion: RankFusionName | RankFusion = Field(default=RankFusionName.RECIPROCAL_RANK_FUSION)


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
