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

"""
Models for Predict API v1.

ATENTION! Keep these models in sync with models on Predict API
"""

from typing import List, Optional

from pydantic import BaseModel, Field


class SentenceSearch(BaseModel):
    vectors: dict[str, List[float]] = Field(
        default_factory=dict,
        description="Sentence vectors for each semantic model",
        min_length=1,
    )
    timings: dict[str, float] = Field(
        default_factory=dict,
        description="Time taken to compute the sentence vector for each semantic model",
        min_length=1,
    )


class Ner(BaseModel):
    text: str
    ner: str
    start: int
    end: int


class TokenSearch(BaseModel):
    tokens: List[Ner] = []
    time: float
    input_tokens: int = 0


class QueryInfo(BaseModel):
    language: Optional[str]
    stop_words: List[str] = Field(default_factory=list)
    semantic_thresholds: dict[str, float] = Field(
        default_factory=dict,
        description="Semantic threshold for each semantic model",
        min_length=1,
    )
    visual_llm: bool
    max_context: int
    entities: Optional[TokenSearch]
    sentence: Optional[SentenceSearch]
    query: str
    rephrased_query: Optional[str] = None


class RerankModel(BaseModel):
    question: str
    user_id: str
    context: dict[str, str] = {}


class RerankResponse(BaseModel):
    context_scores: dict[str, float] = Field(description="Scores for each context given by the reranker")
