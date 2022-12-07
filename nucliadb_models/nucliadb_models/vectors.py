from typing import Dict, List, Optional, Tuple

from pydantic import BaseModel

from nucliadb_models import FieldID

UserVectorPosition = Tuple[int, int]


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
