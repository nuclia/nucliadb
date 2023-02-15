"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.message
import google.protobuf.timestamp_pb2
import nucliadb_protos.knowledgebox_pb2
import nucliadb_protos.resources_pb2
import typing
import typing_extensions
from nucliadb_protos.knowledgebox_pb2 import (
    CONFLICT as CONFLICT,
    CleanedKnowledgeBoxResponse as CleanedKnowledgeBoxResponse,
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
    VectorSet as VectorSet,
    VectorSets as VectorSets,
    Widget as Widget,
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

from nucliadb_protos.writer_pb2 import (
    Audit as Audit,
    BinaryData as BinaryData,
    BinaryMetadata as BinaryMetadata,
    BrokerMessage as BrokerMessage,
    CreateShadowShardRequest as CreateShadowShardRequest,
    DelEntitiesRequest as DelEntitiesRequest,
    DelLabelsRequest as DelLabelsRequest,
    DelVectorSetRequest as DelVectorSetRequest,
    DeleteShadowShardRequest as DeleteShadowShardRequest,
    DetWidgetsRequest as DetWidgetsRequest,
    Error as Error,
    ExportRequest as ExportRequest,
    FileRequest as FileRequest,
    FileUploaded as FileUploaded,
    GetEntitiesGroupRequest as GetEntitiesGroupRequest,
    GetEntitiesGroupResponse as GetEntitiesGroupResponse,
    GetEntitiesRequest as GetEntitiesRequest,
    GetEntitiesResponse as GetEntitiesResponse,
    GetLabelSetRequest as GetLabelSetRequest,
    GetLabelSetResponse as GetLabelSetResponse,
    GetLabelsRequest as GetLabelsRequest,
    GetLabelsResponse as GetLabelsResponse,
    GetVectorSetsRequest as GetVectorSetsRequest,
    GetVectorSetsResponse as GetVectorSetsResponse,
    GetWidgetRequest as GetWidgetRequest,
    GetWidgetResponse as GetWidgetResponse,
    GetWidgetsRequest as GetWidgetsRequest,
    GetWidgetsResponse as GetWidgetsResponse,
    IndexResource as IndexResource,
    IndexStatus as IndexStatus,
    ListMembersRequest as ListMembersRequest,
    ListMembersResponse as ListMembersResponse,
    Member as Member,
    MergeEntitiesRequest as MergeEntitiesRequest,
    Notification as Notification,
    OpStatusWriter as OpStatusWriter,
    ResourceFieldExistsResponse as ResourceFieldExistsResponse,
    ResourceFieldId as ResourceFieldId,
    ResourceIdRequest as ResourceIdRequest,
    ResourceIdResponse as ResourceIdResponse,
    SetEntitiesRequest as SetEntitiesRequest,
    SetLabelsRequest as SetLabelsRequest,
    SetVectorSetRequest as SetVectorSetRequest,
    SetVectorsRequest as SetVectorsRequest,
    SetVectorsResponse as SetVectorsResponse,
    SetWidgetsRequest as SetWidgetsRequest,
    ShadowShard as ShadowShard,
    ShadowShardResponse as ShadowShardResponse,
    ShardObject as ShardObject,
    ShardReplica as ShardReplica,
    Shards as Shards,
    UploadBinaryData as UploadBinaryData,
    WriterStatusRequest as WriterStatusRequest,
    WriterStatusResponse as WriterStatusResponse,
)


DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

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

class TrainLabels(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    RESOURCE_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    @property
    def resource(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.resources_pb2.Classification]: ...
    @property
    def field(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.resources_pb2.Classification]: ...
    @property
    def paragraph(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.resources_pb2.Classification]: ...
    def __init__(self,
        *,
        resource: typing.Optional[typing.Iterable[nucliadb_protos.resources_pb2.Classification]] = ...,
        field: typing.Optional[typing.Iterable[nucliadb_protos.resources_pb2.Classification]] = ...,
        paragraph: typing.Optional[typing.Iterable[nucliadb_protos.resources_pb2.Classification]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","paragraph",b"paragraph","resource",b"resource"]) -> None: ...
global___TrainLabels = TrainLabels

class Position(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    start: builtins.int
    end: builtins.int
    def __init__(self,
        *,
        start: builtins.int = ...,
        end: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["end",b"end","start",b"start"]) -> None: ...
global___Position = Position

class EntityPositions(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ENTITY_FIELD_NUMBER: builtins.int
    POSITIONS_FIELD_NUMBER: builtins.int
    entity: typing.Text
    @property
    def positions(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Position]: ...
    def __init__(self,
        *,
        entity: typing.Text = ...,
        positions: typing.Optional[typing.Iterable[global___Position]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["entity",b"entity","positions",b"positions"]) -> None: ...
global___EntityPositions = EntityPositions

class TrainMetadata(google.protobuf.message.Message):
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

    class EntityPositionsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___EntityPositions: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___EntityPositions] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    TEXT_FIELD_NUMBER: builtins.int
    ENTITIES_FIELD_NUMBER: builtins.int
    ENTITY_POSITIONS_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    VECTOR_FIELD_NUMBER: builtins.int
    text: typing.Text
    @property
    def entities(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, typing.Text]: ...
    @property
    def entity_positions(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___EntityPositions]: ...
    @property
    def labels(self) -> global___TrainLabels: ...
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    def __init__(self,
        *,
        text: typing.Text = ...,
        entities: typing.Optional[typing.Mapping[typing.Text, typing.Text]] = ...,
        entity_positions: typing.Optional[typing.Mapping[typing.Text, global___EntityPositions]] = ...,
        labels: typing.Optional[global___TrainLabels] = ...,
        vector: typing.Optional[typing.Iterable[builtins.float]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["labels",b"labels"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["entities",b"entities","entity_positions",b"entity_positions","labels",b"labels","text",b"text","vector",b"vector"]) -> None: ...
global___TrainMetadata = TrainMetadata

class GetInfoRequest(google.protobuf.message.Message):
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
global___GetInfoRequest = GetInfoRequest

class GetLabelsetsCountRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    PARAGRAPH_LABELSETS_FIELD_NUMBER: builtins.int
    RESOURCE_LABELSETS_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    @property
    def paragraph_labelsets(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def resource_labelsets(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        paragraph_labelsets: typing.Optional[typing.Iterable[typing.Text]] = ...,
        resource_labelsets: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","paragraph_labelsets",b"paragraph_labelsets","resource_labelsets",b"resource_labelsets"]) -> None: ...
global___GetLabelsetsCountRequest = GetLabelsetsCountRequest

class GetResourcesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    SIZE_FIELD_NUMBER: builtins.int
    RANDOM_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    size: builtins.int
    random: builtins.bool
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        size: builtins.int = ...,
        random: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["kb",b"kb","metadata",b"metadata","random",b"random","size",b"size"]) -> None: ...
global___GetResourcesRequest = GetResourcesRequest

class GetParagraphsRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    SIZE_FIELD_NUMBER: builtins.int
    RANDOM_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    size: builtins.int
    random: builtins.bool
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        size: builtins.int = ...,
        random: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata","random",b"random","size",b"size","uuid",b"uuid"]) -> None: ...
global___GetParagraphsRequest = GetParagraphsRequest

class GetSentencesRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    SIZE_FIELD_NUMBER: builtins.int
    RANDOM_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    size: builtins.int
    random: builtins.bool
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        size: builtins.int = ...,
        random: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata","random",b"random","size",b"size","uuid",b"uuid"]) -> None: ...
global___GetSentencesRequest = GetSentencesRequest

class GetFieldsRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KB_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    SIZE_FIELD_NUMBER: builtins.int
    RANDOM_FIELD_NUMBER: builtins.int
    @property
    def kb(self) -> nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID: ...
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    @property
    def metadata(self) -> global___EnabledMetadata: ...
    size: builtins.int
    random: builtins.bool
    def __init__(self,
        *,
        kb: typing.Optional[nucliadb_protos.knowledgebox_pb2.KnowledgeBoxID] = ...,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        metadata: typing.Optional[global___EnabledMetadata] = ...,
        size: builtins.int = ...,
        random: builtins.bool = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","kb",b"kb","metadata",b"metadata","random",b"random","size",b"size","uuid",b"uuid"]) -> None: ...
global___GetFieldsRequest = GetFieldsRequest

class TrainInfo(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    RESOURCES_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    PARAGRAPHS_FIELD_NUMBER: builtins.int
    SENTENCES_FIELD_NUMBER: builtins.int
    resources: builtins.int
    fields: builtins.int
    paragraphs: builtins.int
    sentences: builtins.int
    def __init__(self,
        *,
        resources: builtins.int = ...,
        fields: builtins.int = ...,
        paragraphs: builtins.int = ...,
        sentences: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["fields",b"fields","paragraphs",b"paragraphs","resources",b"resources","sentences",b"sentences"]) -> None: ...
global___TrainInfo = TrainInfo

class TrainSentence(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    SENTENCE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    paragraph: typing.Text
    sentence: typing.Text
    @property
    def metadata(self) -> global___TrainMetadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        paragraph: typing.Text = ...,
        sentence: typing.Text = ...,
        metadata: typing.Optional[global___TrainMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata","paragraph",b"paragraph","sentence",b"sentence","uuid",b"uuid"]) -> None: ...
global___TrainSentence = TrainSentence

class TrainParagraph(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    PARAGRAPH_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    paragraph: typing.Text
    @property
    def metadata(self) -> global___TrainMetadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        paragraph: typing.Text = ...,
        metadata: typing.Optional[global___TrainMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata","paragraph",b"paragraph","uuid",b"uuid"]) -> None: ...
global___TrainParagraph = TrainParagraph

class TrainField(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    UUID_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    SUBFIELD_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    uuid: typing.Text
    @property
    def field(self) -> nucliadb_protos.resources_pb2.FieldID: ...
    subfield: typing.Text
    @property
    def metadata(self) -> global___TrainMetadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        field: typing.Optional[nucliadb_protos.resources_pb2.FieldID] = ...,
        subfield: typing.Text = ...,
        metadata: typing.Optional[global___TrainMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["field",b"field","metadata",b"metadata","subfield",b"subfield","uuid",b"uuid"]) -> None: ...
global___TrainField = TrainField

class TrainResource(google.protobuf.message.Message):
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
    def metadata(self) -> global___TrainMetadata: ...
    def __init__(self,
        *,
        uuid: typing.Text = ...,
        title: typing.Text = ...,
        icon: typing.Text = ...,
        slug: typing.Text = ...,
        created: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        modified: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        metadata: typing.Optional[global___TrainMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["created",b"created","metadata",b"metadata","modified",b"modified"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["created",b"created","icon",b"icon","metadata",b"metadata","modified",b"modified","slug",b"slug","title",b"title","uuid",b"uuid"]) -> None: ...
global___TrainResource = TrainResource

class LabelsetCount(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class ParagraphsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        value: builtins.int
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: builtins.int = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    class ResourcesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        value: builtins.int
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: builtins.int = ...,
            ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    PARAGRAPHS_FIELD_NUMBER: builtins.int
    RESOURCES_FIELD_NUMBER: builtins.int
    @property
    def paragraphs(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, builtins.int]: ...
    @property
    def resources(self) -> google.protobuf.internal.containers.ScalarMap[typing.Text, builtins.int]: ...
    def __init__(self,
        *,
        paragraphs: typing.Optional[typing.Mapping[typing.Text, builtins.int]] = ...,
        resources: typing.Optional[typing.Mapping[typing.Text, builtins.int]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["paragraphs",b"paragraphs","resources",b"resources"]) -> None: ...
global___LabelsetCount = LabelsetCount

class LabelsetsCount(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class LabelsetsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___LabelsetCount: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___LabelsetCount] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    LABELSETS_FIELD_NUMBER: builtins.int
    @property
    def labelsets(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___LabelsetCount]: ...
    def __init__(self,
        *,
        labelsets: typing.Optional[typing.Mapping[typing.Text, global___LabelsetCount]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["labelsets",b"labelsets"]) -> None: ...
global___LabelsetsCount = LabelsetsCount
