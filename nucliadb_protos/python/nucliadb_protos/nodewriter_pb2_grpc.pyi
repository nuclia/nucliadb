"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import grpc
import nucliadb_protos.noderesources_pb2
import nucliadb_protos.nodewriter_pb2
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery as EmptyQuery,
    EmptyResponse as EmptyResponse,
    IndexMetadata as IndexMetadata,
    IndexParagraph as IndexParagraph,
    IndexParagraphs as IndexParagraphs,
    NodeMetadata as NodeMetadata,
    ParagraphMetadata as ParagraphMetadata,
    Position as Position,
    Resource as Resource,
    ResourceID as ResourceID,
    SentenceMetadata as SentenceMetadata,
    Shard as Shard,
    ShardCreated as ShardCreated,
    ShardId as ShardId,
    ShardIds as ShardIds,
    ShardMetadata as ShardMetadata,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
    VectorSetID as VectorSetID,
    VectorSetList as VectorSetList,
)

class NodeWriterStub:
    def __init__(self, channel: grpc.Channel) -> None: ...
    NewShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodewriter_pb2.NewShardRequest,
        nucliadb_protos.noderesources_pb2.ShardCreated,
    ]
    DeleteShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.noderesources_pb2.ShardId,
    ]
    ListShards: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.EmptyQuery,
        nucliadb_protos.noderesources_pb2.ShardIds,
    ]
    GC: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodewriter_pb2.GarbageCollectorResponse,
    ]
    SetResource: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.Resource,
        nucliadb_protos.nodewriter_pb2.OpStatus,
    ]
    RemoveResource: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ResourceID,
        nucliadb_protos.nodewriter_pb2.OpStatus,
    ]
    AddVectorSet: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodewriter_pb2.NewVectorSetRequest,
        nucliadb_protos.nodewriter_pb2.OpStatus,
    ]
    RemoveVectorSet: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.VectorSetID,
        nucliadb_protos.nodewriter_pb2.OpStatus,
    ]
    ListVectorSets: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.noderesources_pb2.VectorSetList,
    ]
    GetMetadata: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.EmptyQuery,
        nucliadb_protos.noderesources_pb2.NodeMetadata,
    ]

class NodeWriterServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def NewShard(
        self,
        request: nucliadb_protos.nodewriter_pb2.NewShardRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.ShardCreated: ...
    @abc.abstractmethod
    def DeleteShard(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.ShardId: ...
    @abc.abstractmethod
    def ListShards(
        self,
        request: nucliadb_protos.noderesources_pb2.EmptyQuery,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.ShardIds: ...
    @abc.abstractmethod
    def GC(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodewriter_pb2.GarbageCollectorResponse: ...
    @abc.abstractmethod
    def SetResource(
        self,
        request: nucliadb_protos.noderesources_pb2.Resource,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodewriter_pb2.OpStatus: ...
    @abc.abstractmethod
    def RemoveResource(
        self,
        request: nucliadb_protos.noderesources_pb2.ResourceID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodewriter_pb2.OpStatus: ...
    @abc.abstractmethod
    def AddVectorSet(
        self,
        request: nucliadb_protos.nodewriter_pb2.NewVectorSetRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodewriter_pb2.OpStatus: ...
    @abc.abstractmethod
    def RemoveVectorSet(
        self,
        request: nucliadb_protos.noderesources_pb2.VectorSetID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodewriter_pb2.OpStatus: ...
    @abc.abstractmethod
    def ListVectorSets(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.VectorSetList: ...
    @abc.abstractmethod
    def GetMetadata(
        self,
        request: nucliadb_protos.noderesources_pb2.EmptyQuery,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.NodeMetadata: ...

def add_NodeWriterServicer_to_server(servicer: NodeWriterServicer, server: grpc.Server) -> None: ...
