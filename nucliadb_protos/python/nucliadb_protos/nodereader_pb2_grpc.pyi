"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import grpc
import nucliadb_protos.nodereader_pb2
import nucliadb_protos.noderesources_pb2
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
)
from nucliadb_protos.utils_pb2 import (
    ExtractedText as ExtractedText,
    Relation as Relation,
    RelationNode as RelationNode,
    Vector as Vector,
    VectorObject as VectorObject,
    Vectors as Vectors,
)

class NodeReaderStub:
    """Implemented at nucliadb_object_storage"""

    def __init__(self, channel: grpc.Channel) -> None: ...
    GetShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.noderesources_pb2.Shard,
    ]
    GetShards: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.EmptyQuery,
        nucliadb_protos.noderesources_pb2.ShardList,
    ]
    DocumentSearch: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.DocumentSearchRequest,
        nucliadb_protos.nodereader_pb2.DocumentSearchResponse,
    ]
    ParagraphSearch: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.ParagraphSearchRequest,
        nucliadb_protos.nodereader_pb2.ParagraphSearchResponse,
    ]
    VectorSearch: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.VectorSearchRequest,
        nucliadb_protos.nodereader_pb2.VectorSearchResponse,
    ]
    RelationSearch: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.RelationSearchRequest,
        nucliadb_protos.nodereader_pb2.RelationSearchResponse,
    ]
    DocumentIds: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]
    ParagraphIds: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]
    VectorIds: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]
    RelationIds: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]
    RelationEdges: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.EdgeList,
    ]
    RelationTypes: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.TypeList,
    ]
    Search: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SearchRequest,
        nucliadb_protos.nodereader_pb2.SearchResponse,
    ]
    Suggest: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SuggestRequest,
        nucliadb_protos.nodereader_pb2.SuggestResponse,
    ]

class NodeReaderServicer(metaclass=abc.ABCMeta):
    """Implemented at nucliadb_object_storage"""

    @abc.abstractmethod
    def GetShard(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.Shard: ...
    @abc.abstractmethod
    def GetShards(
        self,
        request: nucliadb_protos.noderesources_pb2.EmptyQuery,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.noderesources_pb2.ShardList: ...
    @abc.abstractmethod
    def DocumentSearch(
        self,
        request: nucliadb_protos.nodereader_pb2.DocumentSearchRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.DocumentSearchResponse: ...
    @abc.abstractmethod
    def ParagraphSearch(
        self,
        request: nucliadb_protos.nodereader_pb2.ParagraphSearchRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.ParagraphSearchResponse: ...
    @abc.abstractmethod
    def VectorSearch(
        self,
        request: nucliadb_protos.nodereader_pb2.VectorSearchRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.VectorSearchResponse: ...
    @abc.abstractmethod
    def RelationSearch(
        self,
        request: nucliadb_protos.nodereader_pb2.RelationSearchRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.RelationSearchResponse: ...
    @abc.abstractmethod
    def DocumentIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.IdCollection: ...
    @abc.abstractmethod
    def ParagraphIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.IdCollection: ...
    @abc.abstractmethod
    def VectorIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.IdCollection: ...
    @abc.abstractmethod
    def RelationIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.IdCollection: ...
    @abc.abstractmethod
    def RelationEdges(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.EdgeList: ...
    @abc.abstractmethod
    def RelationTypes(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.TypeList: ...
    @abc.abstractmethod
    def Search(
        self,
        request: nucliadb_protos.nodereader_pb2.SearchRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.SearchResponse: ...
    @abc.abstractmethod
    def Suggest(
        self,
        request: nucliadb_protos.nodereader_pb2.SuggestRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.nodereader_pb2.SuggestResponse: ...

def add_NodeReaderServicer_to_server(servicer: NodeReaderServicer, server: grpc.Server) -> None: ...
