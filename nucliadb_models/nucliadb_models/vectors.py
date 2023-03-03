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
        if self.value == self.COSINE:
            return PBVectorSimilarity.Cosine
        elif self.value == self.DOT:
            return PBVectorSimilarity.Dot
        else:
            raise ValueError("Unknown similarity")

    @classmethod
    def from_pb(cls, message: PBVectorSimilarity.ValueType):
        if message == PBVectorSimilarity.Cosine:
            return cls("cosine")
        elif message == PBVectorSimilarity.Dot:
            return cls("dot")
        else:
            raise ValueError("Unknown similarity")


class VectorSet(BaseModel):
    dimension: int
    similarity: Optional[VectorSimilarity]

    @classmethod
    def from_message(cls, message: PBVectorSet):
        return cls(
            dimension=message.dimension,
            similarity=VectorSimilarity.from_pb(message.similarity),
        )


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
