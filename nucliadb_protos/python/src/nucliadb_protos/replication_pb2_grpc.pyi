"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

import abc
import collections.abc
import grpc
import grpc.aio
import nucliadb_protos.noderesources_pb2
import nucliadb_protos.replication_pb2
import typing
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery as EmptyQuery,
    EmptyResponse as EmptyResponse,
    IndexMetadata as IndexMetadata,
    IndexParagraph as IndexParagraph,
    IndexParagraphs as IndexParagraphs,
    IndexRelation as IndexRelation,
    IndexRelations as IndexRelations,
    NodeMetadata as NodeMetadata,
    ParagraphMetadata as ParagraphMetadata,
    Position as Position,
    Representation as Representation,
    Resource as Resource,
    ResourceID as ResourceID,
    SentenceMetadata as SentenceMetadata,
    Shard as Shard,
    ShardCreated as ShardCreated,
    ShardId as ShardId,
    ShardIds as ShardIds,
    ShardMetadata as ShardMetadata,
    StringList as StringList,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
    VectorSetID as VectorSetID,
    VectorSetList as VectorSetList,
    VectorsetSentences as VectorsetSentences,
)

_T = typing.TypeVar("_T")

class _MaybeAsyncIterator(collections.abc.AsyncIterator[_T], collections.abc.Iterator[_T], metaclass=abc.ABCMeta): ...

class _ServicerContext(grpc.ServicerContext, grpc.aio.ServicerContext):  # type: ignore[misc, type-arg]
    ...

class ReplicationServiceStub:
    def __init__(self, channel: typing.Union[grpc.Channel, grpc.aio.Channel]) -> None: ...
    CheckReplicationState: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.replication_pb2.SecondaryCheckReplicationStateRequest,
        nucliadb_protos.replication_pb2.PrimaryCheckReplicationStateResponse,
    ]
    """Shard replication RPCs"""

    ReplicateShard: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.replication_pb2.ReplicateShardRequest,
        nucliadb_protos.replication_pb2.ReplicateShardResponse,
    ]

    GetMetadata: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.EmptyQuery,
        nucliadb_protos.noderesources_pb2.NodeMetadata,
    ]

class ReplicationServiceAsyncStub:
    CheckReplicationState: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.replication_pb2.SecondaryCheckReplicationStateRequest,
        nucliadb_protos.replication_pb2.PrimaryCheckReplicationStateResponse,
    ]
    """Shard replication RPCs"""

    ReplicateShard: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.replication_pb2.ReplicateShardRequest,
        nucliadb_protos.replication_pb2.ReplicateShardResponse,
    ]

    GetMetadata: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.EmptyQuery,
        nucliadb_protos.noderesources_pb2.NodeMetadata,
    ]

class ReplicationServiceServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def CheckReplicationState(
        self,
        request: nucliadb_protos.replication_pb2.SecondaryCheckReplicationStateRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.replication_pb2.PrimaryCheckReplicationStateResponse, collections.abc.Awaitable[nucliadb_protos.replication_pb2.PrimaryCheckReplicationStateResponse]]:
        """Shard replication RPCs"""

    @abc.abstractmethod
    def ReplicateShard(
        self,
        request: nucliadb_protos.replication_pb2.ReplicateShardRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.replication_pb2.ReplicateShardResponse], collections.abc.AsyncIterator[nucliadb_protos.replication_pb2.ReplicateShardResponse]]: ...

    @abc.abstractmethod
    def GetMetadata(
        self,
        request: nucliadb_protos.noderesources_pb2.EmptyQuery,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.noderesources_pb2.NodeMetadata, collections.abc.Awaitable[nucliadb_protos.noderesources_pb2.NodeMetadata]]: ...

def add_ReplicationServiceServicer_to_server(servicer: ReplicationServiceServicer, server: typing.Union[grpc.Server, grpc.aio.Server]) -> None: ...
