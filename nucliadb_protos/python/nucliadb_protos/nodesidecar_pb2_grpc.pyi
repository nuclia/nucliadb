"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import grpc
import nucliadb_protos.noderesources_pb2
import nucliadb_protos.nodesidecar_pb2
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
    ShardCleaned as ShardCleaned,
    ShardCreated as ShardCreated,
    ShardId as ShardId,
    ShardIds as ShardIds,
    ShardMetadata as ShardMetadata,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
    VectorSetID as VectorSetID,
    VectorSetList as VectorSetList,
)

class NodeSidecarStub:
    def __init__(self, channel: grpc.Channel) -> None: ...
    GetCount: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodesidecar_pb2.Counter,
    ]

class NodeSidecarServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def GetCount(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodesidecar_pb2.Counter: ...

def add_NodeSidecarServicer_to_server(servicer: NodeSidecarServicer, server: grpc.Server) -> None: ...
