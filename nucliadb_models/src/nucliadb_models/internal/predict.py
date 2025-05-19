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
