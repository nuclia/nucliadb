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
