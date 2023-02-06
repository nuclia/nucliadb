from typing import Dict, List, Optional, Tuple, Type, TypeVar

from google.protobuf.json_format import MessageToDict
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


class VectorSet(BaseModel):
    dimension: int


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
