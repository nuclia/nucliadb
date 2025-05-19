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
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field


class VectorSimilarity(str, Enum):
    COSINE = "cosine"
    DOT = "dot"


class SemanticModelMetadata(BaseModel):
    """
    Metadata of the semantic model associated to the KB
    """

    similarity_function: VectorSimilarity = Field(
        description="Vector similarity algorithm that is applied on search"
    )
    vector_dimension: Optional[int] = Field(
        None, description="Dimension of the indexed vectors/embeddings"
    )

    # We no longer need this field as we're fetching the minimum
    # semantic score from the query endpoint at search time
    default_min_score: Optional[float] = Field(description="Deprecated", default=None)
