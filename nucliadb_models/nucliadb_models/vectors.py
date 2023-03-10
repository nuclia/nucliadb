from enum import Enum
from typing import Dict, List, Optional, Tuple, Type, TypeVar

from google.protobuf.json_format import MessageToDict
from nucliadb_protos.utils_pb2 import VectorSimilarity as PBVectorSimilarity
from nucliadb_protos.writer_pb2 import VectorSet as PBVectorSet
from pydantic import BaseModel

from nucliadb_models import FieldID
from nucliadb_protos import resources_pb2

UserVectorPosition = Tuple[int, int]
_T = TypeVar("_T")

# Managing vectors


class UserVector(BaseModel):
    vector: List[float]
    positions: Optional[UserVectorPosition] = None


class UserVectorList(BaseModel):
    vectors: List[str]


class UserVectorWrapper(BaseModel):
    vectors: Optional[Dict[str, Dict[str, UserVector]]] = None  # vectorsets
    vectors_to_delete: Optional[Dict[str, UserVectorList]] = None
    field: FieldID


UserVectorsWrapper = List[UserVectorWrapper]


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


class VectorSet(BaseModel):
    dimension: int
    similarity: Optional[VectorSimilarity]

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


# Resource rendering


class GetUserVector(BaseModel):
    vector: List[float]
    labels: Optional[List[str]] = None
    start: int
    end: int


class UserVectors(BaseModel):
    vectors: Optional[Dict[str, GetUserVector]]


class UserVectorSet(BaseModel):
    vectors: Optional[Dict[str, UserVectors]]

    @classmethod
    def from_message(cls: Type[_T], message: resources_pb2.UserVectorSet) -> _T:
        return cls(
            **MessageToDict(
                message,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )
