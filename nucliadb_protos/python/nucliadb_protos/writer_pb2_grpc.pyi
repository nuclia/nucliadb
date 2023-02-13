"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import collections.abc
import grpc
import nucliadb_protos.knowledgebox_pb2
import nucliadb_protos.writer_pb2
from nucliadb_protos.knowledgebox_pb2 import (
    CONFLICT as CONFLICT,
    CleanedKnowledgeBoxResponse as CleanedKnowledgeBoxResponse,
    DeleteKnowledgeBoxResponse as DeleteKnowledgeBoxResponse,
    DeletedEntitiesGroups as DeletedEntitiesGroups,
    ERROR as ERROR,
    EntitiesGroup as EntitiesGroup,
    Entity as Entity,
    GCKnowledgeBoxResponse as GCKnowledgeBoxResponse,
    KnowledgeBox as KnowledgeBox,
    KnowledgeBoxConfig as KnowledgeBoxConfig,
    KnowledgeBoxID as KnowledgeBoxID,
    KnowledgeBoxNew as KnowledgeBoxNew,
    KnowledgeBoxPrefix as KnowledgeBoxPrefix,
    KnowledgeBoxResponseStatus as KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate as KnowledgeBoxUpdate,
    Label as Label,
    LabelSet as LabelSet,
    Labels as Labels,
    NOTFOUND as NOTFOUND,
    NewKnowledgeBoxResponse as NewKnowledgeBoxResponse,
    OK as OK,
    Synonyms as Synonyms,
    TermSynonyms as TermSynonyms,
    UpdateKnowledgeBoxResponse as UpdateKnowledgeBoxResponse,
    VectorSet as VectorSet,
    VectorSets as VectorSets,
    Widget as Widget,
)
from nucliadb_protos.noderesources_pb2 import (
    EmptyQuery as EmptyQuery,
    EmptyResponse as EmptyResponse,
    IndexMetadata as IndexMetadata,
    IndexParagraph as IndexParagraph,
    IndexParagraphs as IndexParagraphs,
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
    ShardList as ShardList,
    ShardMetadata as ShardMetadata,
    TextInformation as TextInformation,
    VectorSentence as VectorSentence,
    VectorSetID as VectorSetID,
    VectorSetList as VectorSetList,
)
from nucliadb_protos.resources_pb2 import (
    Basic as Basic,
    Block as Block,
    CONVERSATION as CONVERSATION,
    Classification as Classification,
    CloudFile as CloudFile,
    ComputedMetadata as ComputedMetadata,
    Conversation as Conversation,
    DATETIME as DATETIME,
    Entity as Entity,
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
    PagePositions as PagePositions,
    Paragraph as Paragraph,
    ParagraphAnnotation as ParagraphAnnotation,
    Position as Position,
    Positions as Positions,
    Relations as Relations,
    RowsPreview as RowsPreview,
    Sentence as Sentence,
    TEXT as TEXT,
    TokenSplit as TokenSplit,
    UserFieldMetadata as UserFieldMetadata,
    UserMetadata as UserMetadata,
    UserVectorsWrapper as UserVectorsWrapper,
)

class WriterStub:
    def __init__(self, channel: grpc.Channel) -> None: ...
    GetKnowledgeBox: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.knowledgebox_pb2.KnowledgeBox,
    ]
    NewKnowledgeBox: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxNew,
        nucliadb_protos.knowledgebox_pb2.NewKnowledgeBoxResponse,
    ]
    DeleteKnowledgeBox: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.knowledgebox_pb2.DeleteKnowledgeBoxResponse,
    ]
    UpdateKnowledgeBox: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxUpdate,
        nucliadb_protos.knowledgebox_pb2.UpdateKnowledgeBoxResponse,
    ]
    CleanAndUpgradeKnowledgeBoxIndex: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.knowledgebox_pb2.CleanedKnowledgeBoxResponse,
    ]
    ListKnowledgeBox: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxPrefix,
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
    ]
    GCKnowledgeBox: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.knowledgebox_pb2.GCKnowledgeBoxResponse,
    ]
    SetVectors: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetVectorsRequest,
        nucliadb_protos.writer_pb2.SetVectorsResponse,
    ]
    ResourceFieldExists: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.ResourceFieldId,
        nucliadb_protos.writer_pb2.ResourceFieldExistsResponse,
    ]
    GetResourceId: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.ResourceIdRequest,
        nucliadb_protos.writer_pb2.ResourceIdResponse,
    ]
    ProcessMessage: grpc.StreamUnaryMultiCallable[
        nucliadb_protos.writer_pb2.BrokerMessage,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    GetLabels: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetLabelsRequest,
        nucliadb_protos.writer_pb2.GetLabelsResponse,
    ]
    """Labels"""
    GetLabelSet: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetLabelSetRequest,
        nucliadb_protos.writer_pb2.GetLabelSetResponse,
    ]
    SetLabels: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetLabelsRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    DelLabels: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.DelLabelsRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    GetVectorSets: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetVectorSetsRequest,
        nucliadb_protos.writer_pb2.GetVectorSetsResponse,
    ]
    """VectorSets"""
    DelVectorSet: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.DelVectorSetRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    SetVectorSet: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetVectorSetRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    GetEntities: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetEntitiesRequest,
        nucliadb_protos.writer_pb2.GetEntitiesResponse,
    ]
    """Entities"""
    GetEntitiesGroup: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetEntitiesGroupRequest,
        nucliadb_protos.writer_pb2.GetEntitiesGroupResponse,
    ]
    SetEntities: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetEntitiesRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    DelEntities: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.DelEntitiesRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    GetWidget: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetWidgetRequest,
        nucliadb_protos.writer_pb2.GetWidgetResponse,
    ]
    """Widgets"""
    GetWidgets: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.GetWidgetsRequest,
        nucliadb_protos.writer_pb2.GetWidgetsResponse,
    ]
    SetWidgets: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetWidgetsRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    DelWidgets: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.DetWidgetsRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    GetSynonyms: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.writer_pb2.GetSynonymsResponse,
    ]
    """Synonyms"""
    SetSynonyms: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.SetSynonymsRequest,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    DelSynonyms: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        nucliadb_protos.writer_pb2.OpStatusWriter,
    ]
    Status: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.WriterStatusRequest,
        nucliadb_protos.writer_pb2.WriterStatusResponse,
    ]
    ListMembers: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.ListMembersRequest,
        nucliadb_protos.writer_pb2.ListMembersResponse,
    ]
    Index: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.IndexResource,
        nucliadb_protos.writer_pb2.IndexStatus,
    ]
    ReIndex: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.IndexResource,
        nucliadb_protos.writer_pb2.IndexStatus,
    ]
    Export: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.writer_pb2.ExportRequest,
        nucliadb_protos.writer_pb2.BrokerMessage,
    ]
    DownloadFile: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.writer_pb2.FileRequest,
        nucliadb_protos.writer_pb2.BinaryData,
    ]
    UploadFile: grpc.StreamUnaryMultiCallable[
        nucliadb_protos.writer_pb2.UploadBinaryData,
        nucliadb_protos.writer_pb2.FileUploaded,
    ]
    CreateShadowShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.CreateShadowShardRequest,
        nucliadb_protos.writer_pb2.ShadowShardResponse,
    ]
    """Shard management"""
    DeleteShadowShard: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.writer_pb2.DeleteShadowShardRequest,
        nucliadb_protos.writer_pb2.ShadowShardResponse,
    ]

class WriterServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def GetKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBox: ...
    @abc.abstractmethod
    def NewKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxNew,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.NewKnowledgeBoxResponse: ...
    @abc.abstractmethod
    def DeleteKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.DeleteKnowledgeBoxResponse: ...
    @abc.abstractmethod
    def UpdateKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxUpdate,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.UpdateKnowledgeBoxResponse: ...
    @abc.abstractmethod
    def CleanAndUpgradeKnowledgeBoxIndex(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.CleanedKnowledgeBoxResponse: ...
    @abc.abstractmethod
    def ListKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxPrefix,
        context: grpc.ServicerContext,
    ) -> collections.abc.Iterator[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID]: ...
    @abc.abstractmethod
    def GCKnowledgeBox(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.knowledgebox_pb2.GCKnowledgeBoxResponse: ...
    @abc.abstractmethod
    def SetVectors(
        self,
        request: nucliadb_protos.writer_pb2.SetVectorsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.SetVectorsResponse: ...
    @abc.abstractmethod
    def ResourceFieldExists(
        self,
        request: nucliadb_protos.writer_pb2.ResourceFieldId,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.ResourceFieldExistsResponse: ...
    @abc.abstractmethod
    def GetResourceId(
        self,
        request: nucliadb_protos.writer_pb2.ResourceIdRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.ResourceIdResponse: ...
    @abc.abstractmethod
    def ProcessMessage(
        self,
        request_iterator: collections.abc.Iterator[nucliadb_protos.writer_pb2.BrokerMessage],
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def GetLabels(
        self,
        request: nucliadb_protos.writer_pb2.GetLabelsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetLabelsResponse:
        """Labels"""
    @abc.abstractmethod
    def GetLabelSet(
        self,
        request: nucliadb_protos.writer_pb2.GetLabelSetRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetLabelSetResponse: ...
    @abc.abstractmethod
    def SetLabels(
        self,
        request: nucliadb_protos.writer_pb2.SetLabelsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def DelLabels(
        self,
        request: nucliadb_protos.writer_pb2.DelLabelsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def GetVectorSets(
        self,
        request: nucliadb_protos.writer_pb2.GetVectorSetsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetVectorSetsResponse:
        """VectorSets"""
    @abc.abstractmethod
    def DelVectorSet(
        self,
        request: nucliadb_protos.writer_pb2.DelVectorSetRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def SetVectorSet(
        self,
        request: nucliadb_protos.writer_pb2.SetVectorSetRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def GetEntities(
        self,
        request: nucliadb_protos.writer_pb2.GetEntitiesRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetEntitiesResponse:
        """Entities"""
    @abc.abstractmethod
    def GetEntitiesGroup(
        self,
        request: nucliadb_protos.writer_pb2.GetEntitiesGroupRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetEntitiesGroupResponse: ...
    @abc.abstractmethod
    def SetEntities(
        self,
        request: nucliadb_protos.writer_pb2.SetEntitiesRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def DelEntities(
        self,
        request: nucliadb_protos.writer_pb2.DelEntitiesRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def GetWidget(
        self,
        request: nucliadb_protos.writer_pb2.GetWidgetRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetWidgetResponse:
        """Widgets"""
    @abc.abstractmethod
    def GetWidgets(
        self,
        request: nucliadb_protos.writer_pb2.GetWidgetsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetWidgetsResponse: ...
    @abc.abstractmethod
    def SetWidgets(
        self,
        request: nucliadb_protos.writer_pb2.SetWidgetsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def DelWidgets(
        self,
        request: nucliadb_protos.writer_pb2.DetWidgetsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def GetSynonyms(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.GetSynonymsResponse:
        """Synonyms"""
    @abc.abstractmethod
    def SetSynonyms(
        self,
        request: nucliadb_protos.writer_pb2.SetSynonymsRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def DelSynonyms(
        self,
        request: nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.OpStatusWriter: ...
    @abc.abstractmethod
    def Status(
        self,
        request: nucliadb_protos.writer_pb2.WriterStatusRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.WriterStatusResponse: ...
    @abc.abstractmethod
    def ListMembers(
        self,
        request: nucliadb_protos.writer_pb2.ListMembersRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.ListMembersResponse: ...
    @abc.abstractmethod
    def Index(
        self,
        request: nucliadb_protos.writer_pb2.IndexResource,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.IndexStatus: ...
    @abc.abstractmethod
    def ReIndex(
        self,
        request: nucliadb_protos.writer_pb2.IndexResource,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.IndexStatus: ...
    @abc.abstractmethod
    def Export(
        self,
        request: nucliadb_protos.writer_pb2.ExportRequest,
        context: grpc.ServicerContext,
    ) -> collections.abc.Iterator[nucliadb_protos.writer_pb2.BrokerMessage]: ...
    @abc.abstractmethod
    def DownloadFile(
        self,
        request: nucliadb_protos.writer_pb2.FileRequest,
        context: grpc.ServicerContext,
    ) -> collections.abc.Iterator[nucliadb_protos.writer_pb2.BinaryData]: ...
    @abc.abstractmethod
    def UploadFile(
        self,
        request_iterator: collections.abc.Iterator[nucliadb_protos.writer_pb2.UploadBinaryData],
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.FileUploaded: ...
    @abc.abstractmethod
    def CreateShadowShard(
        self,
        request: nucliadb_protos.writer_pb2.CreateShadowShardRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.ShadowShardResponse:
        """Shard management"""
    @abc.abstractmethod
    def DeleteShadowShard(
        self,
        request: nucliadb_protos.writer_pb2.DeleteShadowShardRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.writer_pb2.ShadowShardResponse: ...

def add_WriterServicer_to_server(servicer: WriterServicer, server: grpc.Server) -> None: ...
