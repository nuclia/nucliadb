from enum import Enum
from typing import Optional

from google.protobuf.json_format import MessageToDict
from pydantic import BaseModel, Field

from nucliadb_protos import knowledgebox_pb2
from nucliadb_protos.utils_pb2 import VectorSimilarity as PBVectorSimilarity

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
        None, description="Dimension of the indexed vectors/embeddings"
    )

    # We no longer need this field as we're fetching the minimum
    # semantic score from the query endpoint at search time
    default_min_score: Optional[float] = Field(description="Deprecated", default=None)

    @classmethod
    def from_message(cls, message: knowledgebox_pb2.SemanticModelMetadata):
        as_dict = MessageToDict(
            message,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )
        as_dict["similarity_function"] = VectorSimilarity.from_message(message.similarity_function)
        return cls(**as_dict)
