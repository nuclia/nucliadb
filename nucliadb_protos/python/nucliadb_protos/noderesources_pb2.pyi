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
import nucliadb_protos.utils_pb2
import typing
import typing_extensions
from nucliadb_protos.utils_pb2 import (
    ExtractedText as ExtractedText,
    JoinGraph as JoinGraph,
    JoinGraphCnx as JoinGraphCnx,
    Relation as Relation,
    RelationMetadata as RelationMetadata,
    RelationNode as RelationNode,
    UserVector as UserVector,
    UserVectorSet as UserVectorSet,
    UserVectors as UserVectors,
    UserVectorsList as UserVectorsList,
    Vector as Vector,
    VectorObject as VectorObject,
    Vectors as Vectors,
)


DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class TextInformation(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    TEXT_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    text: typing.Text
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        text: typing.Text = ...,
        labels: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["labels",b"labels","text",b"text"]) -> None: ...
global___TextInformation = TextInformation

class IndexMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    MODIFIED_FIELD_NUMBER: builtins.int
    CREATED_FIELD_NUMBER: builtins.int
    @property
    def modified(self) -> google.protobuf.timestamp_pb2.Timestamp:
        """Tantivy doc & para"""
        pass
    @property
    def created(self) -> google.protobuf.timestamp_pb2.Timestamp:
        """Tantivy doc & para"""
        pass
    def __init__(self,
        *,
        modified: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        created: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["created",b"created","modified",b"modified"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["created",b"created","modified",b"modified"]) -> None: ...
global___IndexMetadata = IndexMetadata

class ShardId(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    ID_FIELD_NUMBER: builtins.int
    id: typing.Text
    def __init__(self,
        *,
        id: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["id",b"id"]) -> None: ...
global___ShardId = ShardId

class ShardIds(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    IDS_FIELD_NUMBER: builtins.int
    @property
    def ids(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___ShardId]: ...
    def __init__(self,
        *,
        ids: typing.Optional[typing.Iterable[global___ShardId]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["ids",b"ids"]) -> None: ...
global___ShardIds = ShardIds

class ShardCreated(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _DocumentService:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _DocumentServiceEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[ShardCreated._DocumentService.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        DOCUMENT_V0: ShardCreated._DocumentService.ValueType  # 0
        DOCUMENT_V1: ShardCreated._DocumentService.ValueType  # 1
    class DocumentService(_DocumentService, metaclass=_DocumentServiceEnumTypeWrapper):
        pass

    DOCUMENT_V0: ShardCreated.DocumentService.ValueType  # 0
    DOCUMENT_V1: ShardCreated.DocumentService.ValueType  # 1

    class _ParagraphService:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _ParagraphServiceEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[ShardCreated._ParagraphService.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        PARAGRAPH_V0: ShardCreated._ParagraphService.ValueType  # 0
        PARAGRAPH_V1: ShardCreated._ParagraphService.ValueType  # 1
    class ParagraphService(_ParagraphService, metaclass=_ParagraphServiceEnumTypeWrapper):
        pass

    PARAGRAPH_V0: ShardCreated.ParagraphService.ValueType  # 0
    PARAGRAPH_V1: ShardCreated.ParagraphService.ValueType  # 1

    class _VectorService:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _VectorServiceEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[ShardCreated._VectorService.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        VECTOR_V0: ShardCreated._VectorService.ValueType  # 0
        VECTOR_V1: ShardCreated._VectorService.ValueType  # 1
    class VectorService(_VectorService, metaclass=_VectorServiceEnumTypeWrapper):
        pass

    VECTOR_V0: ShardCreated.VectorService.ValueType  # 0
    VECTOR_V1: ShardCreated.VectorService.ValueType  # 1

    class _RelationService:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _RelationServiceEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[ShardCreated._RelationService.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        RELATION_V0: ShardCreated._RelationService.ValueType  # 0
        RELATION_V1: ShardCreated._RelationService.ValueType  # 1
    class RelationService(_RelationService, metaclass=_RelationServiceEnumTypeWrapper):
        pass

    RELATION_V0: ShardCreated.RelationService.ValueType  # 0
    RELATION_V1: ShardCreated.RelationService.ValueType  # 1

    ID_FIELD_NUMBER: builtins.int
    DOCUMENT_SERVICE_FIELD_NUMBER: builtins.int
    PARAGRAPH_SERVICE_FIELD_NUMBER: builtins.int
    VECTOR_SERVICE_FIELD_NUMBER: builtins.int
    RELATION_SERVICE_FIELD_NUMBER: builtins.int
    id: typing.Text
    document_service: global___ShardCreated.DocumentService.ValueType
    paragraph_service: global___ShardCreated.ParagraphService.ValueType
    vector_service: global___ShardCreated.VectorService.ValueType
    relation_service: global___ShardCreated.RelationService.ValueType
    def __init__(self,
        *,
        id: typing.Text = ...,
        document_service: global___ShardCreated.DocumentService.ValueType = ...,
        paragraph_service: global___ShardCreated.ParagraphService.ValueType = ...,
        vector_service: global___ShardCreated.VectorService.ValueType = ...,
        relation_service: global___ShardCreated.RelationService.ValueType = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["document_service",b"document_service","id",b"id","paragraph_service",b"paragraph_service","relation_service",b"relation_service","vector_service",b"vector_service"]) -> None: ...
global___ShardCreated = ShardCreated

class ShardCleaned(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    DOCUMENT_SERVICE_FIELD_NUMBER: builtins.int
    PARAGRAPH_SERVICE_FIELD_NUMBER: builtins.int
    VECTOR_SERVICE_FIELD_NUMBER: builtins.int
    RELATION_SERVICE_FIELD_NUMBER: builtins.int
    document_service: global___ShardCreated.DocumentService.ValueType
    paragraph_service: global___ShardCreated.ParagraphService.ValueType
    vector_service: global___ShardCreated.VectorService.ValueType
    relation_service: global___ShardCreated.RelationService.ValueType
    def __init__(self,
        *,
        document_service: global___ShardCreated.DocumentService.ValueType = ...,
        paragraph_service: global___ShardCreated.ParagraphService.ValueType = ...,
        vector_service: global___ShardCreated.VectorService.ValueType = ...,
        relation_service: global___ShardCreated.RelationService.ValueType = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["document_service",b"document_service","paragraph_service",b"paragraph_service","relation_service",b"relation_service","vector_service",b"vector_service"]) -> None: ...
global___ShardCleaned = ShardCleaned

class ResourceID(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SHARD_ID_FIELD_NUMBER: builtins.int
    UUID_FIELD_NUMBER: builtins.int
    shard_id: typing.Text
    uuid: typing.Text
    def __init__(self,
        *,
        shard_id: typing.Text = ...,
        uuid: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["shard_id",b"shard_id","uuid",b"uuid"]) -> None: ...
global___ResourceID = ResourceID

class Shard(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    METADATA_FIELD_NUMBER: builtins.int
    SHARD_ID_FIELD_NUMBER: builtins.int
    RESOURCES_FIELD_NUMBER: builtins.int
    PARAGRAPHS_FIELD_NUMBER: builtins.int
    SENTENCES_FIELD_NUMBER: builtins.int
    @property
    def metadata(self) -> global___ShardMetadata: ...
    shard_id: typing.Text
    resources: builtins.int
    paragraphs: builtins.int
    sentences: builtins.int
    def __init__(self,
        *,
        metadata: typing.Optional[global___ShardMetadata] = ...,
        shard_id: typing.Text = ...,
        resources: builtins.int = ...,
        paragraphs: builtins.int = ...,
        sentences: builtins.int = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["metadata",b"metadata","paragraphs",b"paragraphs","resources",b"resources","sentences",b"sentences","shard_id",b"shard_id"]) -> None: ...
global___Shard = Shard

class ShardList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SHARDS_FIELD_NUMBER: builtins.int
    @property
    def shards(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Shard]: ...
    def __init__(self,
        *,
        shards: typing.Optional[typing.Iterable[global___Shard]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["shards",b"shards"]) -> None: ...
global___ShardList = ShardList

class EmptyResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___EmptyResponse = EmptyResponse

class EmptyQuery(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    def __init__(self,
        ) -> None: ...
global___EmptyQuery = EmptyQuery

class VectorSentence(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    VECTOR_FIELD_NUMBER: builtins.int
    @property
    def vector(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.float]: ...
    def __init__(self,
        *,
        vector: typing.Optional[typing.Iterable[builtins.float]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["vector",b"vector"]) -> None: ...
global___VectorSentence = VectorSentence

class ParagraphPosition(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    INDEX_FIELD_NUMBER: builtins.int
    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    PAGE_NUMBER_FIELD_NUMBER: builtins.int
    START_SECONDS_FIELD_NUMBER: builtins.int
    END_SECONDS_FIELD_NUMBER: builtins.int
    index: builtins.int
    start: builtins.int
    end: builtins.int
    page_number: builtins.int
    """For pdfs/documents only"""

    @property
    def start_seconds(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.int]:
        """For multimedia only"""
        pass
    @property
    def end_seconds(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.int]: ...
    def __init__(self,
        *,
        index: builtins.int = ...,
        start: builtins.int = ...,
        end: builtins.int = ...,
        page_number: builtins.int = ...,
        start_seconds: typing.Optional[typing.Iterable[builtins.int]] = ...,
        end_seconds: typing.Optional[typing.Iterable[builtins.int]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["end",b"end","end_seconds",b"end_seconds","index",b"index","page_number",b"page_number","start",b"start","start_seconds",b"start_seconds"]) -> None: ...
global___ParagraphPosition = ParagraphPosition

class ParagraphMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    POSITION_FIELD_NUMBER: builtins.int
    @property
    def position(self) -> global___ParagraphPosition: ...
    def __init__(self,
        *,
        position: typing.Optional[global___ParagraphPosition] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["position",b"position"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["position",b"position"]) -> None: ...
global___ParagraphMetadata = ParagraphMetadata

class IndexParagraph(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class SentencesEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___VectorSentence: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___VectorSentence] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    START_FIELD_NUMBER: builtins.int
    END_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    SENTENCES_FIELD_NUMBER: builtins.int
    FIELD_FIELD_NUMBER: builtins.int
    SPLIT_FIELD_NUMBER: builtins.int
    INDEX_FIELD_NUMBER: builtins.int
    REPEATED_IN_FIELD_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    start: builtins.int
    """Start end position in field text"""

    end: builtins.int
    """Start end position in field text"""

    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Paragraph specific labels"""
        pass
    @property
    def sentences(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___VectorSentence]:
        """key is full id for vectors"""
        pass
    field: typing.Text
    split: typing.Text
    """split were it belongs"""

    index: builtins.int
    repeated_in_field: builtins.bool
    @property
    def metadata(self) -> global___ParagraphMetadata: ...
    def __init__(self,
        *,
        start: builtins.int = ...,
        end: builtins.int = ...,
        labels: typing.Optional[typing.Iterable[typing.Text]] = ...,
        sentences: typing.Optional[typing.Mapping[typing.Text, global___VectorSentence]] = ...,
        field: typing.Text = ...,
        split: typing.Text = ...,
        index: builtins.int = ...,
        repeated_in_field: builtins.bool = ...,
        metadata: typing.Optional[global___ParagraphMetadata] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata",b"metadata"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["end",b"end","field",b"field","index",b"index","labels",b"labels","metadata",b"metadata","repeated_in_field",b"repeated_in_field","sentences",b"sentences","split",b"split","start",b"start"]) -> None: ...
global___IndexParagraph = IndexParagraph

class VectorSetID(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SHARD_FIELD_NUMBER: builtins.int
    VECTORSET_FIELD_NUMBER: builtins.int
    @property
    def shard(self) -> global___ShardId: ...
    vectorset: typing.Text
    def __init__(self,
        *,
        shard: typing.Optional[global___ShardId] = ...,
        vectorset: typing.Text = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["shard",b"shard"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["shard",b"shard","vectorset",b"vectorset"]) -> None: ...
global___VectorSetID = VectorSetID

class VectorSetList(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SHARD_FIELD_NUMBER: builtins.int
    VECTORSET_FIELD_NUMBER: builtins.int
    @property
    def shard(self) -> global___ShardId: ...
    @property
    def vectorset(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    def __init__(self,
        *,
        shard: typing.Optional[global___ShardId] = ...,
        vectorset: typing.Optional[typing.Iterable[typing.Text]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["shard",b"shard"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["shard",b"shard","vectorset",b"vectorset"]) -> None: ...
global___VectorSetList = VectorSetList

class IndexParagraphs(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class ParagraphsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___IndexParagraph: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___IndexParagraph] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    PARAGRAPHS_FIELD_NUMBER: builtins.int
    @property
    def paragraphs(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___IndexParagraph]:
        """id of the paragraph f"{self.rid}/{field_key}/{paragraph.start}-{paragraph.end}" """
        pass
    def __init__(self,
        *,
        paragraphs: typing.Optional[typing.Mapping[typing.Text, global___IndexParagraph]] = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["paragraphs",b"paragraphs"]) -> None: ...
global___IndexParagraphs = IndexParagraphs

class Resource(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _ResourceStatus:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _ResourceStatusEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[Resource._ResourceStatus.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        PROCESSED: Resource._ResourceStatus.ValueType  # 0
        EMPTY: Resource._ResourceStatus.ValueType  # 1
        ERROR: Resource._ResourceStatus.ValueType  # 2
        DELETE: Resource._ResourceStatus.ValueType  # 3
        PENDING: Resource._ResourceStatus.ValueType  # 4
    class ResourceStatus(_ResourceStatus, metaclass=_ResourceStatusEnumTypeWrapper):
        pass

    PROCESSED: Resource.ResourceStatus.ValueType  # 0
    EMPTY: Resource.ResourceStatus.ValueType  # 1
    ERROR: Resource.ResourceStatus.ValueType  # 2
    DELETE: Resource.ResourceStatus.ValueType  # 3
    PENDING: Resource.ResourceStatus.ValueType  # 4

    class TextsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___TextInformation: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___TextInformation] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    class ParagraphsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> global___IndexParagraphs: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[global___IndexParagraphs] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    class VectorsEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> nucliadb_protos.utils_pb2.UserVectors: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[nucliadb_protos.utils_pb2.UserVectors] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    class VectorsToDeleteEntry(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor
        KEY_FIELD_NUMBER: builtins.int
        VALUE_FIELD_NUMBER: builtins.int
        key: typing.Text
        @property
        def value(self) -> nucliadb_protos.utils_pb2.UserVectorsList: ...
        def __init__(self,
            *,
            key: typing.Text = ...,
            value: typing.Optional[nucliadb_protos.utils_pb2.UserVectorsList] = ...,
            ) -> None: ...
        def HasField(self, field_name: typing_extensions.Literal["value",b"value"]) -> builtins.bool: ...
        def ClearField(self, field_name: typing_extensions.Literal["key",b"key","value",b"value"]) -> None: ...

    RESOURCE_FIELD_NUMBER: builtins.int
    METADATA_FIELD_NUMBER: builtins.int
    TEXTS_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    STATUS_FIELD_NUMBER: builtins.int
    PARAGRAPHS_FIELD_NUMBER: builtins.int
    PARAGRAPHS_TO_DELETE_FIELD_NUMBER: builtins.int
    SENTENCES_TO_DELETE_FIELD_NUMBER: builtins.int
    RELATIONS_FIELD_NUMBER: builtins.int
    RELATIONS_TO_DELETE_FIELD_NUMBER: builtins.int
    SHARD_ID_FIELD_NUMBER: builtins.int
    VECTORS_FIELD_NUMBER: builtins.int
    VECTORS_TO_DELETE_FIELD_NUMBER: builtins.int
    @property
    def resource(self) -> global___ResourceID: ...
    @property
    def metadata(self) -> global___IndexMetadata: ...
    @property
    def texts(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___TextInformation]:
        """Doc index
        Tantivy doc filled by field allways full
        """
        pass
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]:
        """Key is RID/FIELDID

        Document labels always serialized full
        """
        pass
    status: global___Resource.ResourceStatus.ValueType
    """Tantivy doc"""

    @property
    def paragraphs(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, global___IndexParagraphs]:
        """Paragraph
        Paragraphs by field
        """
        pass
    @property
    def paragraphs_to_delete(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def sentences_to_delete(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def relations(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.utils_pb2.Relation]:
        """Relations"""
        pass
    @property
    def relations_to_delete(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.utils_pb2.Relation]: ...
    shard_id: typing.Text
    @property
    def vectors(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, nucliadb_protos.utils_pb2.UserVectors]:
        """vectorset is the key"""
        pass
    @property
    def vectors_to_delete(self) -> google.protobuf.internal.containers.MessageMap[typing.Text, nucliadb_protos.utils_pb2.UserVectorsList]:
        """Vectorset prefix vector id"""
        pass
    def __init__(self,
        *,
        resource: typing.Optional[global___ResourceID] = ...,
        metadata: typing.Optional[global___IndexMetadata] = ...,
        texts: typing.Optional[typing.Mapping[typing.Text, global___TextInformation]] = ...,
        labels: typing.Optional[typing.Iterable[typing.Text]] = ...,
        status: global___Resource.ResourceStatus.ValueType = ...,
        paragraphs: typing.Optional[typing.Mapping[typing.Text, global___IndexParagraphs]] = ...,
        paragraphs_to_delete: typing.Optional[typing.Iterable[typing.Text]] = ...,
        sentences_to_delete: typing.Optional[typing.Iterable[typing.Text]] = ...,
        relations: typing.Optional[typing.Iterable[nucliadb_protos.utils_pb2.Relation]] = ...,
        relations_to_delete: typing.Optional[typing.Iterable[nucliadb_protos.utils_pb2.Relation]] = ...,
        shard_id: typing.Text = ...,
        vectors: typing.Optional[typing.Mapping[typing.Text, nucliadb_protos.utils_pb2.UserVectors]] = ...,
        vectors_to_delete: typing.Optional[typing.Mapping[typing.Text, nucliadb_protos.utils_pb2.UserVectorsList]] = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["metadata",b"metadata","resource",b"resource"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["labels",b"labels","metadata",b"metadata","paragraphs",b"paragraphs","paragraphs_to_delete",b"paragraphs_to_delete","relations",b"relations","relations_to_delete",b"relations_to_delete","resource",b"resource","sentences_to_delete",b"sentences_to_delete","shard_id",b"shard_id","status",b"status","texts",b"texts","vectors",b"vectors","vectors_to_delete",b"vectors_to_delete"]) -> None: ...
global___Resource = Resource

class ShardMetadata(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    KBID_FIELD_NUMBER: builtins.int
    kbid: typing.Text
    def __init__(self,
        *,
        kbid: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["kbid",b"kbid"]) -> None: ...
global___ShardMetadata = ShardMetadata
