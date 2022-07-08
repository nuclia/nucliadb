"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import abc
import grpc
import nucliadb_protos.train_pb2
import typing
from nucliadb_protos.knowledgebox_pb2 import (
    CONFLICT as CONFLICT,
    DeleteKnowledgeBoxResponse as DeleteKnowledgeBoxResponse,
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
    UpdateKnowledgeBoxResponse as UpdateKnowledgeBoxResponse,
    Widget as Widget,
)

from nucliadb_protos.resources_pb2 import (
    Basic as Basic,
    Block as Block,
    CONVERSATION as CONVERSATION,
    Classification as Classification,
    CloudFile as CloudFile,
    Conversation as Conversation,
    DATETIME as DATETIME,
    Entity as Entity,
    ExtractedTextWrapper as ExtractedTextWrapper,
    ExtractedVectorsWrapper as ExtractedVectorsWrapper,
    FILE as FILE,
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
    NestedPosition as NestedPosition,
    Origin as Origin,
    PagePositions as PagePositions,
    Paragraph as Paragraph,
    Relations as Relations,
    RowsPreview as RowsPreview,
    Sentence as Sentence,
    TEXT as TEXT,
    TokenSplit as TokenSplit,
    UserFieldMetadata as UserFieldMetadata,
    UserMetadata as UserMetadata,
)


class TrainStub:
    def __init__(self, channel: grpc.Channel) -> None: ...
    GetSentences: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetSentencesRequest,
        nucliadb_protos.train_pb2.Sentence]

    GetParagraphs: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetParagraphsRequest,
        nucliadb_protos.train_pb2.Paragraph]

    GetFields: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetFieldsRequest,
        nucliadb_protos.train_pb2.Field]

    GetResources: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetResourcesRequest,
        nucliadb_protos.train_pb2.Resource]

    GetOntology: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetOntologyRequest,
        nucliadb_protos.train_pb2.Ontology]

    GetEntities: grpc.UnaryUnaryMultiCallable[
        nucliadb_protos.train_pb2.GetEntitiesRequest,
        nucliadb_protos.train_pb2.Entities]


class TrainServicer(metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def GetSentences(self,
        request: nucliadb_protos.train_pb2.GetSentencesRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Sentence]: ...

    @abc.abstractmethod
    def GetParagraphs(self,
        request: nucliadb_protos.train_pb2.GetParagraphsRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Paragraph]: ...

    @abc.abstractmethod
    def GetFields(self,
        request: nucliadb_protos.train_pb2.GetFieldsRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Field]: ...

    @abc.abstractmethod
    def GetResources(self,
        request: nucliadb_protos.train_pb2.GetResourcesRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Resource]: ...

    @abc.abstractmethod
    def GetOntology(self,
        request: nucliadb_protos.train_pb2.GetOntologyRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.train_pb2.Ontology: ...

    @abc.abstractmethod
    def GetEntities(self,
        request: nucliadb_protos.train_pb2.GetEntitiesRequest,
        context: grpc.ServicerContext,
    ) -> nucliadb_protos.train_pb2.Entities: ...


def add_TrainServicer_to_server(servicer: TrainServicer, server: grpc.Server) -> None: ...
