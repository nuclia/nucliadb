# This is an automatically generated file, please do not change
# gen by protobuf_to_pydantic[v0.2.6.2](https://github.com/so1n/protobuf_to_pydantic)
# Protobuf Version: 4.25.3
# Pydantic Version: 1.10.14
import typing
from datetime import datetime
from enum import IntEnum

from google.protobuf.message import Message  # type: ignore
from pydantic import BaseModel, Field

from .audit_p2p import AuditField
from .knowledgebox_p2p import (
    EntitiesGroup,
    KnowledgeBoxID,
    Labels,
    SemanticModelMetadata,
    VectorSets,
)
from .noderesources_p2p import ShardCreated
from .nodewriter_p2p import VectorType
from .resources_p2p import (
    Basic,
    Extra,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldQuestionAnswerWrapper,
    FieldType,
    FileExtractedData,
    LargeComputedMetadataWrapper,
    LinkExtractedData,
    Origin,
    UserVectorsWrapper,
)
from .utils_p2p import Relation, ReleaseChannel, Security, VectorSimilarity


class NotificationSource(IntEnum):
    UNSET = 0
    WRITER = 1
    PROCESSOR = 2


class Audit(BaseModel):
    class Source(IntEnum):
        HTTP = 0
        DASHBOARD = 1
        DESKTOP = 2

    user: str = Field(default="")
    when: datetime = Field(default_factory=datetime.now)
    origin: str = Field(default="")
    source: "Audit.Source" = Field(default=0)
    kbid: str = Field(default="")
    uuid: str = Field(default="")
    message_source: MessageSource = Field(default=0)
    field_metadata: typing.List[FieldID] = Field(default_factory=list)
    audit_fields: typing.List[AuditField] = Field(default_factory=list)


class Error(BaseModel):
    class ErrorCode(IntEnum):
        GENERIC = 0
        EXTRACT = 1
        PROCESS = 2

    field: str = Field(default="")
    field_type: FieldType = Field(default=0)
    error: str = Field(default="")
    code: "Error.ErrorCode" = Field(default=0)


class BrokerMessage(BaseModel):
    class MessageType(IntEnum):
        AUTOCOMMIT = 0
        MULTI = 1
        COMMIT = 2
        ROLLBACK = 3
        DELETE = 4

    class MessageSource(IntEnum):
        WRITER = 0
        PROCESSOR = 1

    kbid: str = Field(default="")
    uuid: str = Field(default="")
    slug: str = Field(default="")
    audit: Audit = Field()
    type: "BrokerMessage.MessageType" = Field(default=0)
    multiid: str = Field(default="")
    basic: Basic = Field()
    origin: Origin = Field()
    relations: typing.List[Relation] = Field(default_factory=list)
    conversations: typing.Dict[str, Conversation] = Field(default_factory=dict)
    layouts: typing.Dict[str, FieldLayout] = Field(default_factory=dict)
    texts: typing.Dict[str, FieldText] = Field(default_factory=dict)
    keywordsets: typing.Dict[str, FieldKeywordset] = Field(default_factory=dict)
    datetimes: typing.Dict[str, FieldDatetime] = Field(default_factory=dict)
    links: typing.Dict[str, FieldLink] = Field(default_factory=dict)
    files: typing.Dict[str, FieldFile] = Field(default_factory=dict)
    link_extracted_data: typing.List[LinkExtractedData] = Field(default_factory=list)
    file_extracted_data: typing.List[FileExtractedData] = Field(default_factory=list)
    extracted_text: typing.List[ExtractedTextWrapper] = Field(default_factory=list)
    field_metadata: typing.List[FieldComputedMetadataWrapper] = Field(
        default_factory=list
    )
    field_vectors: typing.List[ExtractedVectorsWrapper] = Field(default_factory=list)
    field_large_metadata: typing.List[LargeComputedMetadataWrapper] = Field(
        default_factory=list
    )
    delete_fields: typing.List[FieldID] = Field(default_factory=list)
    origin_seq: int = Field(default=0)
    slow_processing_time: float = Field(default=0.0)
    pre_processing_time: float = Field(default=0.0)
    done_time: datetime = Field(default_factory=datetime.now)
    txseqid: int = Field(default=0)
    errors: typing.List[Error] = Field(default_factory=list)
    processing_id: str = Field(default="")
    source: "BrokerMessage.MessageSource" = Field(default=0)
    account_seq: int = Field(default=0)
    user_vectors: typing.List[UserVectorsWrapper] = Field(default_factory=list)
    reindex: bool = Field(default=False)
    extra: Extra = Field()
    question_answers: typing.List[FieldQuestionAnswerWrapper] = Field(
        default_factory=list
    )
    security: Security = Field()


class BrokerMessageBlobReference(BaseModel):
    kbid: str = Field(default="")
    uuid: str = Field(default="")
    storage_key: str = Field(default="")


class WriterStatusResponse(BaseModel):
    knowledgeboxes: typing.List[str] = Field(default_factory=list)
    msgid: typing.Dict[str, int] = Field(default_factory=dict)


class WriterStatusRequest(BaseModel):
    pass


class NewEntitiesGroupRequest(BaseModel):
    kb: KnowledgeBoxID = Field()
    group: str = Field(default="")
    entities: EntitiesGroup = Field()


class NewEntitiesGroupResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        ERROR = 1
        KB_NOT_FOUND = 2
        ALREADY_EXISTS = 3

    status: "NewEntitiesGroupResponse.Status" = Field(default=0)


class SetEntitiesRequest(BaseModel):
    kb: KnowledgeBoxID = Field()
    group: str = Field(default="")
    entities: EntitiesGroup = Field()


class UpdateEntitiesGroupRequest(BaseModel):
    kb: KnowledgeBoxID = Field()
    group: str = Field(default="")
    add: typing.Dict[str, Entity] = Field(default_factory=dict)
    update: typing.Dict[str, Entity] = Field(default_factory=dict)
    delete: typing.List[str] = Field(default_factory=list)
    title: str = Field(default="")
    color: str = Field(default="")


class UpdateEntitiesGroupResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        ERROR = 1
        KB_NOT_FOUND = 2
        ENTITIES_GROUP_NOT_FOUND = 3

    status: "UpdateEntitiesGroupResponse.Status" = Field(default=0)


class ListEntitiesGroupsRequest(BaseModel):
    kb: KnowledgeBoxID = Field()


class ListEntitiesGroupsResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        NOTFOUND = 1
        ERROR = 2

    groups: typing.Dict[str, EntitiesGroupSummary] = Field(default_factory=dict)
    status: "ListEntitiesGroupsResponse.Status" = Field(default=0)


class GetEntitiesRequest(BaseModel):
    kb: KnowledgeBoxID = Field()


class GetEntitiesResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        NOTFOUND = 1
        ERROR = 2

    kb: KnowledgeBoxID = Field()
    groups: typing.Dict[str, EntitiesGroup] = Field(default_factory=dict)
    status: "GetEntitiesResponse.Status" = Field(default=0)


class DelEntitiesRequest(BaseModel):
    kb: KnowledgeBoxID = Field()
    group: str = Field(default="")


class MergeEntitiesRequest(BaseModel):
    class EntityID(BaseModel):
        group: str = Field(default="")
        entity: str = Field(default="")

    kb: KnowledgeBoxID = Field()
    to: "MergeEntitiesRequest.EntityID" = Field()


class GetLabelsResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        NOTFOUND = 1

    kb: KnowledgeBoxID = Field()
    labels: Labels = Field()
    status: "GetLabelsResponse.Status" = Field(default=0)


class GetLabelsRequest(BaseModel):
    kb: KnowledgeBoxID = Field()


class GetEntitiesGroupRequest(BaseModel):
    kb: KnowledgeBoxID = Field()
    group: str = Field(default="")


class GetEntitiesGroupResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        KB_NOT_FOUND = 1
        ENTITIES_GROUP_NOT_FOUND = 2
        ERROR = 3

    kb: KnowledgeBoxID = Field()
    group: EntitiesGroup = Field()
    status: "GetEntitiesGroupResponse.Status" = Field(default=0)


class GetVectorSetsRequest(BaseModel):
    kb: KnowledgeBoxID = Field()


class GetVectorSetsResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        NOTFOUND = 1
        ERROR = 2

    kb: KnowledgeBoxID = Field()
    vectorsets: VectorSets = Field()
    status: "GetVectorSetsResponse.Status" = Field(default=0)


class OpStatusWriter(BaseModel):
    class Status(IntEnum):
        OK = 0
        ERROR = 1
        NOTFOUND = 2

    status: "OpStatusWriter.Status" = Field(default=0)


class Notification(BaseModel):
    class Action(IntEnum):
        COMMIT = 0
        ABORT = 1
        INDEXED = 2

    class WriteType(IntEnum):
        UNSET = 0
        CREATED = 1
        MODIFIED = 2
        DELETED = 3

    partition: int = Field(default=0)
    multi: str = Field(default="")
    uuid: str = Field(default="")
    kbid: str = Field(default="")
    seqid: int = Field(default=0)
    action: "Notification.Action" = Field(default=0)
    write_type: "Notification.WriteType" = Field(default=0)
    message: BrokerMessage = Field()
    source: NotificationSource = Field(default=0)
    processing_errors: bool = Field(default=False)
    message_audit: Audit = Field()


class Member(BaseModel):
    class Type(IntEnum):
        IO = 0
        SEARCH = 1
        INGEST = 2
        TRAIN = 3
        UNKNOWN = 4

    id: str = Field(default="")
    listen_address: str = Field(default="")
    is_self: bool = Field(default=False)
    type: "Member.Type" = Field(default=0)
    dummy: bool = Field(default=False)
    load_score: float = Field(default=0.0)
    shard_count: int = Field(default=0)
    primary_id: str = Field(default="")


class ListMembersRequest(BaseModel):
    pass


class ListMembersResponse(BaseModel):
    members: typing.List[Member] = Field(default_factory=list)


class ShardReplica(BaseModel):
    shard: ShardCreated = Field()
    node: str = Field(default="")


class ShardObject(BaseModel):
    shard: str = Field(default="")
    replicas: typing.List[ShardReplica] = Field(default_factory=list)
    timestamp: datetime = Field(default_factory=datetime.now)
    read_only: bool = Field(default=False)


class Shards(BaseModel):
    shards: typing.List[ShardObject] = Field(default_factory=list)
    kbid: str = Field(default="")
    actual: int = Field(default=0)
    similarity: VectorSimilarity = Field(default=0)
    model: SemanticModelMetadata = Field()
    release_channel: ReleaseChannel = Field(default=0)
    extra: typing.Dict[str, str] = Field(default_factory=dict)


class IndexResource(BaseModel):
    kbid: str = Field(default="")
    rid: str = Field(default="")
    reindex_vectors: bool = Field(default=False)


class IndexStatus(BaseModel):
    pass


class SynonymsRequest(BaseModel):
    kbid: str = Field(default="")


class NewVectorSetRequest(BaseModel):
    kbid: str = Field(default="")
    vectorset_id: str = Field(default="")
    vector_type: VectorType = Field(default=0)
    similarity: VectorSimilarity = Field(default=0)
    vector_dimension: int = Field(default=0)
    normalize_vectors: bool = Field(default=False)
    matryoshka_dimensions: typing.List[int] = Field(default_factory=list)


class NewVectorSetResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        ERROR = 1

    status: "NewVectorSetResponse.Status" = Field(default=0)
    details: str = Field(default="")


class DelVectorSetRequest(BaseModel):
    kbid: str = Field(default="")
    vectorset_id: str = Field(default="")


class DelVectorSetResponse(BaseModel):
    class Status(IntEnum):
        OK = 0
        ERROR = 1

    status: "DelVectorSetResponse.Status" = Field(default=0)
    details: str = Field(default="")
