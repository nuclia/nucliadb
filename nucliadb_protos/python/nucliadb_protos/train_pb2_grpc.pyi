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


class TrainStub:
    def __init__(self, channel: grpc.Channel) -> None: ...
    GetSentences: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetSentenceRequest,
        nucliadb_protos.train_pb2.Sentence]

    GetParagraphs: grpc.UnaryStreamMultiCallable[
        nucliadb_protos.train_pb2.GetParagraphRequest,
        nucliadb_protos.train_pb2.Paragraph]

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
        request: nucliadb_protos.train_pb2.GetSentenceRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Sentence]: ...

    @abc.abstractmethod
    def GetParagraphs(self,
        request: nucliadb_protos.train_pb2.GetParagraphRequest,
        context: grpc.ServicerContext,
    ) -> typing.Iterator[nucliadb_protos.train_pb2.Paragraph]: ...

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
