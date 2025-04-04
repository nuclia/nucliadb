"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

import abc
import collections.abc
import grpc
import grpc.aio
import nucliadb_protos.nodereader_pb2
import nucliadb_protos.noderesources_pb2
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
from nucliadb_protos.utils_pb2 import (
    COSINE as COSINE,
    DOT as DOT,
    EXPERIMENTAL as EXPERIMENTAL,
    ExtractedText as ExtractedText,
    Relation as Relation,
    RelationMetadata as RelationMetadata,
    RelationNode as RelationNode,
    ReleaseChannel as ReleaseChannel,
    STABLE as STABLE,
    Security as Security,
    UserVector as UserVector,
    UserVectorSet as UserVectorSet,
    UserVectors as UserVectors,
    UserVectorsList as UserVectorsList,
    Vector as Vector,
    VectorObject as VectorObject,
    VectorSimilarity as VectorSimilarity,
    Vectors as Vectors,
)

_T = typing.TypeVar("_T")

class _MaybeAsyncIterator(collections.abc.AsyncIterator[_T], collections.abc.Iterator[_T], metaclass=abc.ABCMeta): ...

class _ServicerContext(grpc.ServicerContext, grpc.aio.ServicerContext):  # type: ignore[misc, type-arg]
    ...

class NodeReaderStub:
    """Implemented at nucliadb_object_storage"""

    def __init__(self, channel: typing.Union[grpc.Channel, grpc.aio.Channel]) -> None: ...
    GetShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.GetShardRequest,
        nucliadb_protos.noderesources_pb2.Shard,
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
        nucliadb_protos.noderesources_pb2.VectorSetID,
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

    Search: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SearchRequest,
        nucliadb_protos.nodereader_pb2.SearchResponse,
    ]

    Suggest: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SuggestRequest,
        nucliadb_protos.nodereader_pb2.SuggestResponse,
    ]

    Paragraphs: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.StreamRequest,
        nucliadb_protos.nodereader_pb2.ParagraphItem,
    ]
    """Streams"""

    Documents: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.StreamRequest,
        nucliadb_protos.nodereader_pb2.DocumentItem,
    ]

    GetShardFiles: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.GetShardFilesRequest,
        nucliadb_protos.nodereader_pb2.ShardFileList,
    ]
    """Shard Download"""

    DownloadShardFile: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.DownloadShardFileRequest,
        nucliadb_protos.nodereader_pb2.ShardFileChunk,
    ]

class NodeReaderAsyncStub:
    """Implemented at nucliadb_object_storage"""

    GetShard: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.GetShardRequest,
        nucliadb_protos.noderesources_pb2.Shard,
    ]

    DocumentIds: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]

    ParagraphIds: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]

    VectorIds: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.VectorSetID,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]

    RelationIds: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.IdCollection,
    ]

    RelationEdges: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.noderesources_pb2.ShardId,
        nucliadb_protos.nodereader_pb2.EdgeList,
    ]

    Search: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SearchRequest,
        nucliadb_protos.nodereader_pb2.SearchResponse,
    ]

    Suggest: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.SuggestRequest,
        nucliadb_protos.nodereader_pb2.SuggestResponse,
    ]

    Paragraphs: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.StreamRequest,
        nucliadb_protos.nodereader_pb2.ParagraphItem,
    ]
    """Streams"""

    Documents: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.StreamRequest,
        nucliadb_protos.nodereader_pb2.DocumentItem,
    ]

    GetShardFiles: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.nodereader_pb2.GetShardFilesRequest,
        nucliadb_protos.nodereader_pb2.ShardFileList,
    ]
    """Shard Download"""

    DownloadShardFile: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.nodereader_pb2.DownloadShardFileRequest,
        nucliadb_protos.nodereader_pb2.ShardFileChunk,
    ]

class NodeReaderServicer(metaclass=abc.ABCMeta):
    """Implemented at nucliadb_object_storage"""

    @abc.abstractmethod
    def GetShard(
        self,
        request: nucliadb_protos.nodereader_pb2.GetShardRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.noderesources_pb2.Shard, collections.abc.Awaitable[nucliadb_protos.noderesources_pb2.Shard]]: ...

    @abc.abstractmethod
    def DocumentIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.IdCollection, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.IdCollection]]: ...

    @abc.abstractmethod
    def ParagraphIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.IdCollection, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.IdCollection]]: ...

    @abc.abstractmethod
    def VectorIds(
        self,
        request: nucliadb_protos.noderesources_pb2.VectorSetID,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.IdCollection, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.IdCollection]]: ...

    @abc.abstractmethod
    def RelationIds(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.IdCollection, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.IdCollection]]: ...

    @abc.abstractmethod
    def RelationEdges(
        self,
        request: nucliadb_protos.noderesources_pb2.ShardId,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.EdgeList, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.EdgeList]]: ...

    @abc.abstractmethod
    def Search(
        self,
        request: nucliadb_protos.nodereader_pb2.SearchRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.SearchResponse, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.SearchResponse]]: ...

    @abc.abstractmethod
    def Suggest(
        self,
        request: nucliadb_protos.nodereader_pb2.SuggestRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.SuggestResponse, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.SuggestResponse]]: ...

    @abc.abstractmethod
    def Paragraphs(
        self,
        request: nucliadb_protos.nodereader_pb2.StreamRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.nodereader_pb2.ParagraphItem], collections.abc.AsyncIterator[nucliadb_protos.nodereader_pb2.ParagraphItem]]:
        """Streams"""

    @abc.abstractmethod
    def Documents(
        self,
        request: nucliadb_protos.nodereader_pb2.StreamRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.nodereader_pb2.DocumentItem], collections.abc.AsyncIterator[nucliadb_protos.nodereader_pb2.DocumentItem]]: ...

    @abc.abstractmethod
    def GetShardFiles(
        self,
        request: nucliadb_protos.nodereader_pb2.GetShardFilesRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.nodereader_pb2.ShardFileList, collections.abc.Awaitable[nucliadb_protos.nodereader_pb2.ShardFileList]]:
        """Shard Download"""

    @abc.abstractmethod
    def DownloadShardFile(
        self,
        request: nucliadb_protos.nodereader_pb2.DownloadShardFileRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.nodereader_pb2.ShardFileChunk], collections.abc.AsyncIterator[nucliadb_protos.nodereader_pb2.ShardFileChunk]]: ...

def add_NodeReaderServicer_to_server(servicer: NodeReaderServicer, server: typing.Union[grpc.Server, grpc.aio.Server]) -> None: ...
