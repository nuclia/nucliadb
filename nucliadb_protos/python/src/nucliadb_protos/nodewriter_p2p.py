# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.6.2](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.3
# Pydantic Version: 1.10.14
import typing
from enum import IntEnum

from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel, Field

from .noderesources_p2p import VectorSetID
from .utils_p2p import ReleaseChannel, VectorSimilarity


class TypeMessage(IntEnum):
    CREATION = 0
    DELETION = 1


class IndexMessageSource(IntEnum):
    PROCESSOR = 0
    WRITER = 1


class VectorType(IntEnum):
    DENSE_F32 = 0


class OpStatus(BaseModel):
    class Status(IntEnum):
        OK = 0
        WARNING = 1
        ERROR = 2

    status: "OpStatus.Status" = Field(default=0)
    detail: str = Field(default="")
    field_count: int = Field(default=0)
    paragraph_count: int = Field(default=0)
    sentence_count: int = Field(default=0)
    shard_id: str = Field(default="")


class IndexMessage(BaseModel):
    node: str = Field(default="")
    shard: str = Field(default="")
    txid: int = Field(default=0)
    resource: str = Field(default="")
    typemessage: TypeMessage = Field(default=0)
    reindex_id: str = Field(default="")
    partition: typing.Optional[str] = Field(default="")
    storage_key: str = Field(default="")
    kbid: str = Field(default="")
    source: IndexMessageSource = Field(default=0)


class GarbageCollectorResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        TRY_LATER = 1

    status: "GarbageCollectorResponse.Status" = Field(default=0)


class VectorIndexConfig(BaseModel):
    similarity: VectorSimilarity = Field(default=0)
    normalize_vectors: bool = Field(default=False)
    vector_type: VectorType = Field(default=0)
    vector_dimension: typing.Optional[int] = Field(default=0)


class NewShardRequest(BaseModel):
    similarity: VectorSimilarity = Field(default=0)
    kbid: str = Field(default="")
    release_channel: ReleaseChannel = Field(default=0)
    normalize_vectors: bool = Field(default=False)
    config: VectorIndexConfig = Field()


class NewVectorSetRequest(BaseModel):
    id: VectorSetID = Field()
    similarity: VectorSimilarity = Field(default=0)
    normalize_vectors: bool = Field(default=False)
    config: VectorIndexConfig = Field()


class MergeResponse(BaseModel):
    class MergeStatus(IntEnum):
        OK = 0

    status: "MergeResponse.MergeStatus" = Field(default=0)
    merged_segments: int = Field(default=0)
    remaining_segments: int = Field(default=0)
