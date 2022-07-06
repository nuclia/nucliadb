"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import google.protobuf.timestamp_pb2
import nucliadb_protos.knowledgebox_pb2
import typing
import typing_extensions
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


DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class GetOntologyRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> None: ...
global___GetOntologyRequest = GetOntologyRequest

class Ontology(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _Status:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _StatusEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Ontology._Status.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        OK: Ontology._Status.ValueType  # 0
        NOTFOUND: Ontology._Status.ValueType  # 1
    class Status(_Status, metaclass=_StatusEnumTypeWrapper):
        pass

    OK: Ontology.Status.ValueType  # 0
    NOTFOUND: Ontology.Status.ValueType  # 1

    KB_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    STATUS_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    @property
    def labels(self) -> nucliadb_protos.knowledgebox_pb2.Labels: ...
    status: global___Ontology.Status.ValueType
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        labels: typing.Optional[nucliadb_protos.knowledgebox_pb2.Labels] = ...,
        status: global___Ontology.Status.ValueType = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb","labels",b"labels"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","labels",b"labels","status",b"status"]) -> None: ...
global___Ontology = Ontology

class GetEntitiesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> None: ...
global___GetEntitiesRequest = GetEntitiesRequest

class Entities(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _Status:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _StatusEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Entities._Status.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        OK: Entities._Status.ValueType  # 0
        NOTFOUND: Entities._Status.ValueType  # 1
    class Status(_Status, metaclass=_StatusEnumTypeWrapper):
        pass

    OK: Entities.Status.ValueType  # 0
    NOTFOUND: Entities.Status.ValueType  # 1

    class GroupsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> nucliadb_protos.knowledgebox_pb2.EntitiesGroup: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[nucliadb_protos.knowledgebox_pb2.EntitiesGroup] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    KB_FIELD_NUMBER: builtins.int
    GROUPS_FIELD_NUMBER: builtins.int
    STATUS_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    @property
    def groups(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, nucliadb_protos.knowledgebox_pb2.EntitiesGroup]: ...
    status: global___Entities.Status.ValueType
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        groups: typing.Optional[typing.Mapping[typing.Text, nucliadb_protos.knowledgebox_pb2.EntitiesGroup]] = ...,
        status: global___Entities.Status.ValueType = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["groups",b"groups","kb",b"kb","status",b"status"]) -> None: ...
global___Entities = Entities

class EnabledMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    TEXT_FIELD_NUMBER: builtins.int
    ENTITIES_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    text: builtins.bool
    entities: builtins.bool
    labels: builtins.bool
    vector: builtins.bool
    def __init__(self,
        *,
        text: builtins.bool = ...,
        entities: builtins.bool = ...,
        labels: builtins.bool = ...,
        vector: builtins.bool = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entities",b"entities","labels",b"labels","text",b"text","vector",b"vector"]) -> None: ...
global___EnabledMetadata = EnabledMetadata

class Metadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class EntitiesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        value: typing.Text
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Text = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    TEXT_FIELD_NUMBER: builtins.int
    ENTITIES_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    text: typing.Text
    @property
    def entities(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, typing.Text]: ...
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    def __init__(self,
        *,
        text: typing.Text = ...,
        entities: typing.Optional[typing.Mapping[typing.Text, typing.Text]] = ...,
        labels: typing.Optional[typing.Iterable[typing.Text]] = ...,
        vector: typing.Optional[typing.Iterable[builtins.float]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entities",b"entities","labels",b"labels","text",b"text","vector",b"vector"]) -> None: ...
global___Metadata = Metadata

class GetResourcesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata"]) -> None: ...
global___GetResourcesRequest = GetResourcesRequest

class GetParagraphRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    uuid: typing.Text
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        uuid: typing.Text = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata","uuid",b"uuid"]) -> None: ...
global___GetParagraphRequest = GetParagraphRequest

class GetSentenceRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    uuid: typing.Text
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        uuid: typing.Text = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata","uuid",b"uuid"]) -> None: ...
global___GetSentenceRequest = GetSentenceRequest

class Sentence(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    SENTENCE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    paragraph: typing.Text
    sentence: typing.Text
    @property
    def metadata(self) -> global___Metadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        paragraph: typing.Text = ...,
        sentence: typing.Text = ...,
        metadata: typing.Optional[global___Metadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata",b"metadata","paragraph",b"paragraph","sentence",b"sentence","uuid",b"uuid"]) -> None: ...
global___Sentence = Sentence

class Paragraph(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    paragraph: typing.Text
    @property
    def metadata(self) -> global___Metadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        paragraph: typing.Text = ...,
        metadata: typing.Optional[global___Metadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata",b"metadata","paragraph",b"paragraph","uuid",b"uuid"]) -> None: ...
global___Paragraph = Paragraph

class Resource(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    TITLE_FIELD_NUMBER: builtins.int
    ICON_FIELD_NUMBER: builtins.int
    SLUG_FIELD_NUMBER: builtins.int
    CREATED_FIELD_NUMBER: builtins.int
    MODIFIED_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    title: typing.Text
    icon: typing.Text
    slug: typing.Text
    @property
    def created(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def modified(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def metadata(self) -> global___Metadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        title: typing.Text = ...,
        icon: typing.Text = ...,
        slug: typing.Text = ...,
        created: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        modified: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        metadata: typing.Optional[global___Metadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["created",b"created","metadata",b"metadata","modified",b"modified"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["created",b"created","icon",b"icon","metadata",b"metadata","modified",b"modified","slug",b"slug","title",b"title","uuid",b"uuid"]) -> None: ...
global___Resource = Resource
