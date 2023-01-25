"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import nucliadb_protos.noderesources_pb2
import nucliadb_protos.utils_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery as EmptyQuery,
    EmptyResponse as EmptyResponse,
    IndexMetadata as IndexMetadata,
    IndexParagraph as IndexParagraph,
    IndexParagraphs as IndexParagraphs,
    ParagraphMetadata as ParagraphMetadata,
    ParagraphPosition as ParagraphPosition,
    Resource as Resource,
    ResourceID as ResourceID,
    Shard as Shard,
    ShardCleaned as ShardCleaned,
    ShardCreated as ShardCreated,
    ShardId as ShardId,
    ShardIds as ShardIds,
    ShardList as ShardList,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
    VectorSetID as VectorSetID,
    VectorSetList as VectorSetList,
)

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class OpStatus(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _Status:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _StatusEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[OpStatus._Status.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        OK: OpStatus._Status.ValueType  # 0
        WARNING: OpStatus._Status.ValueType  # 1
        ERROR: OpStatus._Status.ValueType  # 2

    class Status(_Status, metaclass=_StatusEnumTypeWrapper): ...
    OK: OpStatus.Status.ValueType  # 0
    WARNING: OpStatus.Status.ValueType  # 1
    ERROR: OpStatus.Status.ValueType  # 2

    STATUS_FIELD_NUMBER: builtins.int
    DETAIL_FIELD_NUMBER: builtins.int
    COUNT_FIELD_NUMBER: builtins.int
    SHARD_ID_FIELD_NUMBER: builtins.int
    status: global___OpStatus.Status.ValueType
    detail: builtins.str
    count: builtins.int
    shard_id: builtins.str
    def __init__(
        self,
        *,
        status: global___OpStatus.Status.ValueType = ...,
        detail: builtins.str = ...,
        count: builtins.int = ...,
        shard_id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["count", b"count", "detail", b"detail", "shard_id", b"shard_id", "status", b"status"]) -> None: ...

global___OpStatus = OpStatus

@typing_extensions.final
class IndexMessage(google.protobuf.message.Message):
    """Implemented at nucliadb_object_storage"""

    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _TypeMessage:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _TypeMessageEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[IndexMessage._TypeMessage.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        CREATION: IndexMessage._TypeMessage.ValueType  # 0
        DELETION: IndexMessage._TypeMessage.ValueType  # 1

    class TypeMessage(_TypeMessage, metaclass=_TypeMessageEnumTypeWrapper): ...
    CREATION: IndexMessage.TypeMessage.ValueType  # 0
    DELETION: IndexMessage.TypeMessage.ValueType  # 1

    NODE_FIELD_NUMBER: builtins.int
    SHARD_FIELD_NUMBER: builtins.int
    TXID_FIELD_NUMBER: builtins.int
    RESOURCE_FIELD_NUMBER: builtins.int
    TYPEMESSAGE_FIELD_NUMBER: builtins.int
    REINDEX_ID_FIELD_NUMBER: builtins.int
    node: builtins.str
    shard: builtins.str
    txid: builtins.int
    resource: builtins.str
    typemessage: global___IndexMessage.TypeMessage.ValueType
    reindex_id: builtins.str
    def __init__(
        self,
        *,
        node: builtins.str = ...,
        shard: builtins.str = ...,
        txid: builtins.int = ...,
        resource: builtins.str = ...,
        typemessage: global___IndexMessage.TypeMessage.ValueType = ...,
        reindex_id: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["node", b"node", "reindex_id", b"reindex_id", "resource", b"resource", "shard", b"shard", "txid", b"txid", "typemessage", b"typemessage"]) -> None: ...

global___IndexMessage = IndexMessage

@typing_extensions.final
class SetGraph(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_ID_FIELD_NUMBER: builtins.int
    GRAPH_FIELD_NUMBER: builtins.int
    @property
    def shard_id(self) -> nucliadb_protos.noderesources_pb2.ShardId: ...
    @property
    def graph(self) -> nucliadb_protos.utils_pb2.JoinGraph: ...
    def __init__(
        self,
        *,
        shard_id: nucliadb_protos.noderesources_pb2.ShardId | None = ...,
        graph: nucliadb_protos.utils_pb2.JoinGraph | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["graph", b"graph", "shard_id", b"shard_id"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["graph", b"graph", "shard_id", b"shard_id"]) -> None: ...

global___SetGraph = SetGraph

@typing_extensions.final
class DeleteGraphNodes(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_ID_FIELD_NUMBER: builtins.int
    NODES_FIELD_NUMBER: builtins.int
    @property
    def shard_id(self) -> nucliadb_protos.noderesources_pb2.ShardId: ...
    @property
    def nodes(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.utils_pb2.RelationNode]: ...
    def __init__(
        self,
        *,
        shard_id: nucliadb_protos.noderesources_pb2.ShardId | None = ...,
        nodes: collections.abc.Iterable[nucliadb_protos.utils_pb2.RelationNode] | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["shard_id", b"shard_id"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["nodes", b"nodes", "shard_id", b"shard_id"]) -> None: ...

global___DeleteGraphNodes = DeleteGraphNodes

@typing_extensions.final
class MoveShardRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_ID_FIELD_NUMBER: builtins.int
    ADDRESS_FIELD_NUMBER: builtins.int
    @property
    def shard_id(self) -> nucliadb_protos.noderesources_pb2.ShardId: ...
    address: builtins.str
    def __init__(
        self,
        *,
        shard_id: nucliadb_protos.noderesources_pb2.ShardId | None = ...,
        address: builtins.str = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["shard_id", b"shard_id"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["address", b"address", "shard_id", b"shard_id"]) -> None: ...

global___MoveShardRequest = MoveShardRequest

@typing_extensions.final
class AcceptShardRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SHARD_ID_FIELD_NUMBER: builtins.int
    PORT_FIELD_NUMBER: builtins.int
    @property
    def shard_id(self) -> nucliadb_protos.noderesources_pb2.ShardId: ...
    port: builtins.int
    def __init__(
        self,
        *,
        shard_id: nucliadb_protos.noderesources_pb2.ShardId | None = ...,
        port: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["shard_id", b"shard_id"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["port", b"port", "shard_id", b"shard_id"]) -> None: ...

global___AcceptShardRequest = AcceptShardRequest

@typing_extensions.final
class Counter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    RESOURCES_FIELD_NUMBER: builtins.int
    resources: builtins.int
    def __init__(
        self,
        *,
        resources: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["resources", b"resources"]) -> None: ...

global___Counter = Counter
