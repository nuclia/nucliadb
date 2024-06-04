"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

import abc
import collections.abc
import grpc
import grpc.aio
import nucliadb_protos.train_pb2
import nucliadb_protos.writer_pb2
import typing
from nucliadb_protos.knowledgebox_pb2 import (
    CONFLICT as CONFLICT,
    DeleteKnowledgeBoxResponse as DeleteKnowledgeBoxResponse,
    DeletedEntitiesGroups as DeletedEntitiesGroups,
    ERROR as ERROR,
    EntitiesGroup as EntitiesGroup,
    EntitiesGroupSummary as EntitiesGroupSummary,
    EntitiesGroups as EntitiesGroups,
    Entity as Entity,
    EntityGroupDuplicateIndex as EntityGroupDuplicateIndex,
    KBConfiguration as KBConfiguration,
    KnowledgeBoxConfig as KnowledgeBoxConfig,
    KnowledgeBoxID as KnowledgeBoxID,
    KnowledgeBoxNew as KnowledgeBoxNew,
    KnowledgeBoxResponseStatus as KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate as KnowledgeBoxUpdate,
    KnowledgeBoxVectorSetsConfig as KnowledgeBoxVectorSetsConfig,
    Label as Label,
    LabelSet as LabelSet,
    Labels as Labels,
    NOTFOUND as NOTFOUND,
    NewKnowledgeBoxResponse as NewKnowledgeBoxResponse,
    OK as OK,
    SemanticModelMetadata as SemanticModelMetadata,
    Synonyms as Synonyms,
    TermSynonyms as TermSynonyms,
    UpdateKnowledgeBoxResponse as UpdateKnowledgeBoxResponse,
    VectorSet as VectorSet,
    VectorSetConfig as VectorSetConfig,
    VectorSets as VectorSets,
)
from nucliadb_protos.resources_pb2 import (
    AllFieldIDs as AllFieldIDs,
    Answers as Answers,
    Basic as Basic,
    Block as Block,
    CONVERSATION as CONVERSATION,
    Classification as Classification,
    CloudFile as CloudFile,
    ComputedMetadata as ComputedMetadata,
    Conversation as Conversation,
    DATETIME as DATETIME,
    Entity as Entity,
    Extra as Extra,
    ExtractedTextWrapper as ExtractedTextWrapper,
    ExtractedVectorsWrapper as ExtractedVectorsWrapper,
    FILE as FILE,
    FieldClassifications as FieldClassifications,
    FieldComputedMetadata as FieldComputedMetadata,
    FieldComputedMetadataWrapper as FieldComputedMetadataWrapper,
    FieldConversation as FieldConversation,
    FieldDatetime as FieldDatetime,
    FieldFile as FieldFile,
    FieldID as FieldID,
    FieldKeywordset as FieldKeywordset,
    FieldLargeMetadata as FieldLargeMetadata,
    FieldLayout as FieldLayout,
    FieldLink as FieldLink,
    FieldMetadata as FieldMetadata,
    FieldQuestionAnswerWrapper as FieldQuestionAnswerWrapper,
    FieldText as FieldText,
    FieldType as FieldType,
    FileExtractedData as FileExtractedData,
    FilePages as FilePages,
    GENERIC as GENERIC,
    KEYWORDSET as KEYWORDSET,
    Keyword as Keyword,
    LAYOUT as LAYOUT,
    LINK as LINK,
    LargeComputedMetadata as LargeComputedMetadata,
    LargeComputedMetadataWrapper as LargeComputedMetadataWrapper,
    LayoutContent as LayoutContent,
    LinkExtractedData as LinkExtractedData,
    Message as Message,
    MessageContent as MessageContent,
    Metadata as Metadata,
    NestedListPosition as NestedListPosition,
    NestedPosition as NestedPosition,
    Origin as Origin,
    PageInformation as PageInformation,
    PagePositions as PagePositions,
    PageSelections as PageSelections,
    PageStructure as PageStructure,
    PageStructurePage as PageStructurePage,
    PageStructureToken as PageStructureToken,
    Paragraph as Paragraph,
    ParagraphAnnotation as ParagraphAnnotation,
    ParagraphRelations as ParagraphRelations,
    Position as Position,
    Positions as Positions,
    Question as Question,
    QuestionAnswer as QuestionAnswer,
    QuestionAnswerAnnotation as QuestionAnswerAnnotation,
    QuestionAnswers as QuestionAnswers,
    Relations as Relations,
    Representation as Representation,
    RowsPreview as RowsPreview,
    Sentence as Sentence,
    TEXT as TEXT,
    TokenSplit as TokenSplit,
    UserFieldMetadata as UserFieldMetadata,
    UserMetadata as UserMetadata,
    UserVectorsWrapper as UserVectorsWrapper,
    VisualSelection as VisualSelection,
)
from nucliadb_protos.writer_pb2 import (
    Audit as Audit,
    BinaryData as BinaryData,
    BinaryMetadata as BinaryMetadata,
    BrokerMessage as BrokerMessage,
    BrokerMessageBlobReference as BrokerMessageBlobReference,
    DelEntitiesRequest as DelEntitiesRequest,
    DelVectorSetRequest as DelVectorSetRequest,
    DelVectorSetResponse as DelVectorSetResponse,
    Error as Error,
    FileRequest as FileRequest,
    FileUploaded as FileUploaded,
    GetEntitiesGroupRequest as GetEntitiesGroupRequest,
    GetEntitiesGroupResponse as GetEntitiesGroupResponse,
    GetEntitiesRequest as GetEntitiesRequest,
    GetEntitiesResponse as GetEntitiesResponse,
    GetLabelsRequest as GetLabelsRequest,
    GetLabelsResponse as GetLabelsResponse,
    GetVectorSetsRequest as GetVectorSetsRequest,
    GetVectorSetsResponse as GetVectorSetsResponse,
    IndexResource as IndexResource,
    IndexStatus as IndexStatus,
    ListEntitiesGroupsRequest as ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse as ListEntitiesGroupsResponse,
    ListMembersRequest as ListMembersRequest,
    ListMembersResponse as ListMembersResponse,
    Member as Member,
    MergeEntitiesRequest as MergeEntitiesRequest,
    NewEntitiesGroupRequest as NewEntitiesGroupRequest,
    NewEntitiesGroupResponse as NewEntitiesGroupResponse,
    NewVectorSetRequest as NewVectorSetRequest,
    NewVectorSetResponse as NewVectorSetResponse,
    Notification as Notification,
    NotificationSource as NotificationSource,
    OpStatusWriter as OpStatusWriter,
    PROCESSOR as PROCESSOR,
    SetEntitiesRequest as SetEntitiesRequest,
    SetVectorsRequest as SetVectorsRequest,
    SetVectorsResponse as SetVectorsResponse,
    ShardObject as ShardObject,
    ShardReplica as ShardReplica,
    Shards as Shards,
    SynonymsRequest as SynonymsRequest,
    UNSET as UNSET,
    UpdateEntitiesGroupRequest as UpdateEntitiesGroupRequest,
    UpdateEntitiesGroupResponse as UpdateEntitiesGroupResponse,
    UploadBinaryData as UploadBinaryData,
    WRITER as WRITER,
    WriterStatusRequest as WriterStatusRequest,
    WriterStatusResponse as WriterStatusResponse,
)

_T = typing.TypeVar("_T")

class _MaybeAsyncIterator(collections.abc.AsyncIterator[_T], collections.abc.Iterator[_T], metaclass=abc.ABCMeta): ...

class _ServicerContext(grpc.ServicerContext, grpc.aio.ServicerContext):  # type: ignore[misc, type-arg]
    ...

class TrainStub:
    def __init__(self, channel: typing.Union[grpc.Channel, grpc.aio.Channel]) -> None: ...
    GetInfo: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetInfoRequest,
        nucliadb_protos.train_pb2.TrainInfo,
    ]

    GetSentences: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetSentencesRequest,
        nucliadb_protos.train_pb2.TrainSentence,
    ]

    GetParagraphs: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetParagraphsRequest,
        nucliadb_protos.train_pb2.TrainParagraph,
    ]

    GetFields: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetFieldsRequest,
        nucliadb_protos.train_pb2.TrainField,
    ]

    GetResources: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetResourcesRequest,
        nucliadb_protos.train_pb2.TrainResource,
    ]

    GetOntology: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetLabelsRequest,
        nucliadb_protos.writer_pb2.GetLabelsResponse,
    ]

    GetEntities: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetEntitiesRequest,
        nucliadb_protos.writer_pb2.GetEntitiesResponse,
    ]

    GetOntologyCount: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetLabelsetsCountRequest,
        nucliadb_protos.train_pb2.LabelsetsCount,
    ]

class TrainAsyncStub:
    GetInfo: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetInfoRequest,
        nucliadb_protos.train_pb2.TrainInfo,
    ]

    GetSentences: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetSentencesRequest,
        nucliadb_protos.train_pb2.TrainSentence,
    ]

    GetParagraphs: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetParagraphsRequest,
        nucliadb_protos.train_pb2.TrainParagraph,
    ]

    GetFields: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetFieldsRequest,
        nucliadb_protos.train_pb2.TrainField,
    ]

    GetResources: grpc.aio.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetResourcesRequest,
        nucliadb_protos.train_pb2.TrainResource,
    ]

    GetOntology: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetLabelsRequest,
        nucliadb_protos.writer_pb2.GetLabelsResponse,
    ]

    GetEntities: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetEntitiesRequest,
        nucliadb_protos.writer_pb2.GetEntitiesResponse,
    ]

    GetOntologyCount: grpc.aio.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetLabelsetsCountRequest,
        nucliadb_protos.train_pb2.LabelsetsCount,
    ]

class TrainServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def GetInfo(
        self,
        request: nucliadb_protos.train_pb2.GetInfoRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.train_pb2.TrainInfo, collections.abc.Awaitable[nucliadb_protos.train_pb2.TrainInfo]]: ...

    @abc.abstractmethod
    def GetSentences(
        self,
        request: nucliadb_protos.train_pb2.GetSentencesRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.train_pb2.TrainSentence], collections.abc.AsyncIterator[nucliadb_protos.train_pb2.TrainSentence]]: ...

    @abc.abstractmethod
    def GetParagraphs(
        self,
        request: nucliadb_protos.train_pb2.GetParagraphsRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.train_pb2.TrainParagraph], collections.abc.AsyncIterator[nucliadb_protos.train_pb2.TrainParagraph]]: ...

    @abc.abstractmethod
    def GetFields(
        self,
        request: nucliadb_protos.train_pb2.GetFieldsRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.train_pb2.TrainField], collections.abc.AsyncIterator[nucliadb_protos.train_pb2.TrainField]]: ...

    @abc.abstractmethod
    def GetResources(
        self,
        request: nucliadb_protos.train_pb2.GetResourcesRequest,
        context: _ServicerContext,
    ) -> typing.Union[collections.abc.Iterator[nucliadb_protos.train_pb2.TrainResource], collections.abc.AsyncIterator[nucliadb_protos.train_pb2.TrainResource]]: ...

    @abc.abstractmethod
    def GetOntology(
        self,
        request: nucliadb_protos.writer_pb2.GetLabelsRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.writer_pb2.GetLabelsResponse, collections.abc.Awaitable[nucliadb_protos.writer_pb2.GetLabelsResponse]]: ...

    @abc.abstractmethod
    def GetEntities(
        self,
        request: nucliadb_protos.writer_pb2.GetEntitiesRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.writer_pb2.GetEntitiesResponse, collections.abc.Awaitable[nucliadb_protos.writer_pb2.GetEntitiesResponse]]: ...

    @abc.abstractmethod
    def GetOntologyCount(
        self,
        request: nucliadb_protos.train_pb2.GetLabelsetsCountRequest,
        context: _ServicerContext,
    ) -> typing.Union[nucliadb_protos.train_pb2.LabelsetsCount, collections.abc.Awaitable[nucliadb_protos.train_pb2.LabelsetsCount]]: ...

def add_TrainServicer_to_server(servicer: TrainServicer, server: typing.Union[grpc.Server, grpc.aio.Server]) -> None: ...
