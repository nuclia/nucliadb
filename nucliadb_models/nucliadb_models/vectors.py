from enum import Enum
from typing import Dict, List, Optional, TypeVar

from google.protobuf.json_format import MessageToDict
from nucliadb_protos.utils_pb2 import VectorSimilarity as PBVectorSimilarity
from nucliadb_protos.writer_pb2 import VectorSet as PBVectorSet
from pydantic import BaseModel, Field, root_validator

from nucliadb_protos import knowledgebox_pb2

_T = TypeVar("_T")

# Managing vectors


class VectorSimilarity(str, Enum):
    COSINE = "cosine"
    DOT = "dot"

    def to_pb(self) -> PBVectorSimilarity.ValueType:
        return VECTOR_SIMILARITY_ENUM_TO_PB[self.value]

    @classmethod
    def from_message(cls, message: PBVectorSimilarity.ValueType):
        return cls(VECTOR_SIMILARITY_PB_TO_ENUM[message])


VECTOR_SIMILARITY_ENUM_TO_PB = {
    VectorSimilarity.COSINE.value: PBVectorSimilarity.COSINE,
    VectorSimilarity.DOT.value: PBVectorSimilarity.DOT,
}
VECTOR_SIMILARITY_PB_TO_ENUM = {v: k for k, v in VECTOR_SIMILARITY_ENUM_TO_PB.items()}


class SemanticModelMetadata(BaseModel):
    """
    Metadata of the semantic model associated to the KB
    """

    similarity_function: VectorSimilarity = Field(
        description="Vector similarity algorithm that is applied on search"
    )
    vector_dimension: Optional[int] = Field(
        description="Dimension of the indexed vectors/embeddings"
    )
    default_min_score: Optional[float] = Field(
        description="Default minimum similarity value at which results are ignored"
    )

    @classmethod
    def from_message(cls, message: knowledgebox_pb2.SemanticModelMetadata):
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        as_dict["similarity_function"] = VectorSimilarity.from_message(
            message.similarity_function
        )
        return cls(**as_dict)


class VectorSet(BaseModel):
    semantic_model: str = Field(description="Id of the semantic model")
    semantic_vector_similarity: VectorSimilarity = Field(
        description="Vector similarity algorithm that is applied on search"
    )
    semantic_vector_size: int = Field(
        description="Dimension of the indexed vectors/embeddings"
    )
    semantic_threshold: float = Field(
        description="Minimum similarity value at which results are ignored"
    )
    semantic_matryoshka_dimensions: Optional[List[int]] = Field(
        default=None, description="Matryoshka dimensions"
    )

    @root_validator(pre=False)
    def validate_dimensions(cls, values):
        vector_size = values.get("semantic_vector_size")
        matryoshka_dimensions = values.get("semantic_matryoshka_dimensions", []) or []
        if (
            len(matryoshka_dimensions) > 0
            and vector_size is not None
            and vector_size not in matryoshka_dimensions
        ):
            raise ValueError(
                f"Semantic vector size {vector_size} is inconsistent with matryoshka dimensions: {matryoshka_dimensions}"
            )
        return values

    @classmethod
    def from_message(cls, message: PBVectorSet):
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        as_dict["similarity"] = VectorSimilarity.from_message(message.similarity)
        return cls(**as_dict)


class VectorSets(BaseModel):
    vectorsets: Dict[str, VectorSet]
