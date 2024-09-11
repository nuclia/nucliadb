"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import collections.abc
import google.protobuf.descriptor
import google.protobuf.internal.containers
import google.protobuf.internal.enum_type_wrapper
import google.protobuf.message
import google.protobuf.timestamp_pb2
import nucliadb_protos.nodereader_pb2
import nucliadb_protos.resources_pb2
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _ClientType:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _ClientTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_ClientType.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    API: _ClientType.ValueType  # 0
    WEB: _ClientType.ValueType  # 1
    WIDGET: _ClientType.ValueType  # 2
    DESKTOP: _ClientType.ValueType  # 3
    DASHBOARD: _ClientType.ValueType  # 4
    CHROME_EXTENSION: _ClientType.ValueType  # 5

class ClientType(_ClientType, metaclass=_ClientTypeEnumTypeWrapper): ...

API: ClientType.ValueType  # 0
WEB: ClientType.ValueType  # 1
WIDGET: ClientType.ValueType  # 2
DESKTOP: ClientType.ValueType  # 3
DASHBOARD: ClientType.ValueType  # 4
CHROME_EXTENSION: ClientType.ValueType  # 5
global___ClientType = ClientType

class _TaskType:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _TaskTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_TaskType.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    CHAT: _TaskType.ValueType  # 0

class TaskType(_TaskType, metaclass=_TaskTypeEnumTypeWrapper): ...

CHAT: TaskType.ValueType  # 0
global___TaskType = TaskType

@typing_extensions.final
class AuditField(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _FieldAction:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _FieldActionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[AuditField._FieldAction.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        ADDED: AuditField._FieldAction.ValueType  # 0
        MODIFIED: AuditField._FieldAction.ValueType  # 1
        DELETED: AuditField._FieldAction.ValueType  # 2

    class FieldAction(_FieldAction, metaclass=_FieldActionEnumTypeWrapper): ...
    ADDED: AuditField.FieldAction.ValueType  # 0
    MODIFIED: AuditField.FieldAction.ValueType  # 1
    DELETED: AuditField.FieldAction.ValueType  # 2

    ACTION_FIELD_NUMBER: builtins.int
    SIZE_FIELD_NUMBER: builtins.int
    SIZE_DELTA_FIELD_NUMBER: builtins.int
    FIELD_ID_FIELD_NUMBER: builtins.int
    FIELD_TYPE_FIELD_NUMBER: builtins.int
    FILENAME_FIELD_NUMBER: builtins.int
    action: global___AuditField.FieldAction.ValueType
    size: builtins.int
    size_delta: builtins.int
    """no longer calculated"""
    field_id: builtins.str
    field_type: nucliadb_protos.resources_pb2.FieldType.ValueType
    filename: builtins.str
    def __init__(
        self,
        *,
        action: global___AuditField.FieldAction.ValueType = ...,
        size: builtins.int = ...,
        size_delta: builtins.int = ...,
        field_id: builtins.str = ...,
        field_type: nucliadb_protos.resources_pb2.FieldType.ValueType = ...,
        filename: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["action", b"action", "field_id", b"field_id", "field_type", b"field_type", "filename", b"filename", "size", b"size", "size_delta", b"size_delta"]) -> None: ...

global___AuditField = AuditField

@typing_extensions.final
class AuditKBCounter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PARAGRAPHS_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    paragraphs: builtins.int
    fields: builtins.int
    def __init__(
        self,
        *,
        paragraphs: builtins.int = ...,
        fields: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["fields", b"fields", "paragraphs", b"paragraphs"]) -> None: ...

global___AuditKBCounter = AuditKBCounter

@typing_extensions.final
class ChatContext(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    AUTHOR_FIELD_NUMBER: builtins.int
    TEXT_FIELD_NUMBER: builtins.int
    author: builtins.str
    text: builtins.str
    def __init__(
        self,
        *,
        author: builtins.str = ...,
        text: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["author", b"author", "text", b"text"]) -> None: ...

global___ChatContext = ChatContext

@typing_extensions.final
class RetrievedContext(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TEXT_BLOCK_ID_FIELD_NUMBER: builtins.int
    TEXT_FIELD_NUMBER: builtins.int
    text_block_id: builtins.str
    text: builtins.str
    def __init__(
        self,
        *,
        text_block_id: builtins.str = ...,
        text: builtins.str = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["text", b"text", "text_block_id", b"text_block_id"]) -> None: ...

global___RetrievedContext = RetrievedContext

@typing_extensions.final
class ChatAudit(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    QUESTION_FIELD_NUMBER: builtins.int
    ANSWER_FIELD_NUMBER: builtins.int
    REPHRASED_QUESTION_FIELD_NUMBER: builtins.int
    CONTEXT_FIELD_NUMBER: builtins.int
    CHAT_CONTEXT_FIELD_NUMBER: builtins.int
    RETRIEVED_CONTEXT_FIELD_NUMBER: builtins.int
    LEARNING_ID_FIELD_NUMBER: builtins.int
    STATUS_CODE_FIELD_NUMBER: builtins.int
    question: builtins.str
    answer: builtins.str
    rephrased_question: builtins.str
    @property
    def context(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___ChatContext]:
        """Conversation from chats"""
    @property
    def chat_context(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___ChatContext]:
        """context retrieved on the current ask"""
    @property
    def retrieved_context(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___RetrievedContext]: ...
    learning_id: builtins.str
    status_code: builtins.int
    def __init__(
        self,
        *,
        question: builtins.str = ...,
        answer: builtins.str | None = ...,
        rephrased_question: builtins.str | None = ...,
        context: collections.abc.Iterable[global___ChatContext] | None = ...,
        chat_context: collections.abc.Iterable[global___ChatContext] | None = ...,
        retrieved_context: collections.abc.Iterable[global___RetrievedContext] | None = ...,
        learning_id: builtins.str = ...,
        status_code: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_answer", b"_answer", "_rephrased_question", b"_rephrased_question", "answer", b"answer", "rephrased_question", b"rephrased_question"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_answer", b"_answer", "_rephrased_question", b"_rephrased_question", "answer", b"answer", "chat_context", b"chat_context", "context", b"context", "learning_id", b"learning_id", "question", b"question", "rephrased_question", b"rephrased_question", "retrieved_context", b"retrieved_context", "status_code", b"status_code"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_answer", b"_answer"]) -> typing_extensions.Literal["answer"] | None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_rephrased_question", b"_rephrased_question"]) -> typing_extensions.Literal["rephrased_question"] | None: ...

global___ChatAudit = ChatAudit

@typing_extensions.final
class FeedbackAudit(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LEARNING_ID_FIELD_NUMBER: builtins.int
    GOOD_FIELD_NUMBER: builtins.int
    TASK_FIELD_NUMBER: builtins.int
    FEEDBACK_FIELD_NUMBER: builtins.int
    learning_id: builtins.str
    good: builtins.bool
    task: global___TaskType.ValueType
    feedback: builtins.str
    def __init__(
        self,
        *,
        learning_id: builtins.str = ...,
        good: builtins.bool = ...,
        task: global___TaskType.ValueType = ...,
        feedback: builtins.str | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_feedback", b"_feedback", "feedback", b"feedback"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_feedback", b"_feedback", "feedback", b"feedback", "good", b"good", "learning_id", b"learning_id", "task", b"task"]) -> None: ...
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_feedback", b"_feedback"]) -> typing_extensions.Literal["feedback"] | None: ...

global___FeedbackAudit = FeedbackAudit

@typing_extensions.final
class AuditRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    class _AuditType:
        ValueType = typing.NewType("ValueType", builtins.int)
        V: typing_extensions.TypeAlias = ValueType

    class _AuditTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[AuditRequest._AuditType.ValueType], builtins.type):  # noqa: F821
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        VISITED: AuditRequest._AuditType.ValueType  # 0
        MODIFIED: AuditRequest._AuditType.ValueType  # 1
        DELETED: AuditRequest._AuditType.ValueType  # 2
        NEW: AuditRequest._AuditType.ValueType  # 3
        STARTED: AuditRequest._AuditType.ValueType  # 4
        STOPPED: AuditRequest._AuditType.ValueType  # 5
        SEARCH: AuditRequest._AuditType.ValueType  # 6
        PROCESSED: AuditRequest._AuditType.ValueType  # 7
        KB_DELETED: AuditRequest._AuditType.ValueType  # 8
        SUGGEST: AuditRequest._AuditType.ValueType  # 9
        INDEXED: AuditRequest._AuditType.ValueType  # 10
        CHAT: AuditRequest._AuditType.ValueType  # 11
        FEEDBACK: AuditRequest._AuditType.ValueType  # 12

    class AuditType(_AuditType, metaclass=_AuditTypeEnumTypeWrapper): ...
    VISITED: AuditRequest.AuditType.ValueType  # 0
    MODIFIED: AuditRequest.AuditType.ValueType  # 1
    DELETED: AuditRequest.AuditType.ValueType  # 2
    NEW: AuditRequest.AuditType.ValueType  # 3
    STARTED: AuditRequest.AuditType.ValueType  # 4
    STOPPED: AuditRequest.AuditType.ValueType  # 5
    SEARCH: AuditRequest.AuditType.ValueType  # 6
    PROCESSED: AuditRequest.AuditType.ValueType  # 7
    KB_DELETED: AuditRequest.AuditType.ValueType  # 8
    SUGGEST: AuditRequest.AuditType.ValueType  # 9
    INDEXED: AuditRequest.AuditType.ValueType  # 10
    CHAT: AuditRequest.AuditType.ValueType  # 11
    FEEDBACK: AuditRequest.AuditType.ValueType  # 12

    TYPE_FIELD_NUMBER: builtins.int
    KBID_FIELD_NUMBER: builtins.int
    USERID_FIELD_NUMBER: builtins.int
    TIME_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    SEARCH_FIELD_NUMBER: builtins.int
    TIMEIT_FIELD_NUMBER: builtins.int
    ORIGIN_FIELD_NUMBER: builtins.int
    RID_FIELD_NUMBER: builtins.int
    TASK_FIELD_NUMBER: builtins.int
    RESOURCES_FIELD_NUMBER: builtins.int
    FIELD_METADATA_FIELD_NUMBER: builtins.int
    FIELDS_AUDIT_FIELD_NUMBER: builtins.int
    CLIENT_TYPE_FIELD_NUMBER: builtins.int
    TRACE_ID_FIELD_NUMBER: builtins.int
    KB_COUNTER_FIELD_NUMBER: builtins.int
    CHAT_FIELD_NUMBER: builtins.int
    SUCCESS_FIELD_NUMBER: builtins.int
    REQUEST_TIME_FIELD_NUMBER: builtins.int
    RETRIEVAL_TIME_FIELD_NUMBER: builtins.int
    GENERATIVE_ANSWER_TIME_FIELD_NUMBER: builtins.int
    GENERATIVE_ANSWER_FIRST_CHUNK_TIME_FIELD_NUMBER: builtins.int
    REPHRASE_TIME_FIELD_NUMBER: builtins.int
    FEEDBACK_FIELD_NUMBER: builtins.int
    type: global___AuditRequest.AuditType.ValueType
    kbid: builtins.str
    userid: builtins.str
    @property
    def time(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def search(self) -> nucliadb_protos.nodereader_pb2.SearchRequest: ...
    timeit: builtins.float
    origin: builtins.str
    rid: builtins.str
    task: builtins.str
    resources: builtins.int
    @property
    def field_metadata(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.resources_pb2.FieldID]: ...
    @property
    def fields_audit(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___AuditField]: ...
    client_type: global___ClientType.ValueType
    trace_id: builtins.str
    @property
    def kb_counter(self) -> global___AuditKBCounter: ...
    @property
    def chat(self) -> global___ChatAudit: ...
    success: builtins.bool
    request_time: builtins.float
    retrieval_time: builtins.float
    generative_answer_time: builtins.float
    generative_answer_first_chunk_time: builtins.float
    rephrase_time: builtins.float
    @property
    def feedback(self) -> global___FeedbackAudit: ...
    def __init__(
        self,
        *,
        type: global___AuditRequest.AuditType.ValueType = ...,
        kbid: builtins.str = ...,
        userid: builtins.str = ...,
        time: google.protobuf.timestamp_pb2.Timestamp | None = ...,
        fields: collections.abc.Iterable[builtins.str] | None = ...,
        search: nucliadb_protos.nodereader_pb2.SearchRequest | None = ...,
        timeit: builtins.float = ...,
        origin: builtins.str = ...,
        rid: builtins.str = ...,
        task: builtins.str = ...,
        resources: builtins.int = ...,
        field_metadata: collections.abc.Iterable[nucliadb_protos.resources_pb2.FieldID] | None = ...,
        fields_audit: collections.abc.Iterable[global___AuditField] | None = ...,
        client_type: global___ClientType.ValueType = ...,
        trace_id: builtins.str = ...,
        kb_counter: global___AuditKBCounter | None = ...,
        chat: global___ChatAudit | None = ...,
        success: builtins.bool = ...,
        request_time: builtins.float = ...,
        retrieval_time: builtins.float | None = ...,
        generative_answer_time: builtins.float | None = ...,
        generative_answer_first_chunk_time: builtins.float | None = ...,
        rephrase_time: builtins.float | None = ...,
        feedback: global___FeedbackAudit | None = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["_generative_answer_first_chunk_time", b"_generative_answer_first_chunk_time", "_generative_answer_time", b"_generative_answer_time", "_rephrase_time", b"_rephrase_time", "_retrieval_time", b"_retrieval_time", "chat", b"chat", "feedback", b"feedback", "generative_answer_first_chunk_time", b"generative_answer_first_chunk_time", "generative_answer_time", b"generative_answer_time", "kb_counter", b"kb_counter", "rephrase_time", b"rephrase_time", "retrieval_time", b"retrieval_time", "search", b"search", "time", b"time"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["_generative_answer_first_chunk_time", b"_generative_answer_first_chunk_time", "_generative_answer_time", b"_generative_answer_time", "_rephrase_time", b"_rephrase_time", "_retrieval_time", b"_retrieval_time", "chat", b"chat", "client_type", b"client_type", "feedback", b"feedback", "field_metadata", b"field_metadata", "fields", b"fields", "fields_audit", b"fields_audit", "generative_answer_first_chunk_time", b"generative_answer_first_chunk_time", "generative_answer_time", b"generative_answer_time", "kb_counter", b"kb_counter", "kbid", b"kbid", "origin", b"origin", "rephrase_time", b"rephrase_time", "request_time", b"request_time", "resources", b"resources", "retrieval_time", b"retrieval_time", "rid", b"rid", "search", b"search", "success", b"success", "task", b"task", "time", b"time", "timeit", b"timeit", "trace_id", b"trace_id", "type", b"type", "userid", b"userid"]) -> None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_generative_answer_first_chunk_time", b"_generative_answer_first_chunk_time"]) -> typing_extensions.Literal["generative_answer_first_chunk_time"] | None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_generative_answer_time", b"_generative_answer_time"]) -> typing_extensions.Literal["generative_answer_time"] | None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_rephrase_time", b"_rephrase_time"]) -> typing_extensions.Literal["rephrase_time"] | None: ...
    @typing.overload
    def WhichOneof(self, oneof_group: typing_extensions.Literal["_retrieval_time", b"_retrieval_time"]) -> typing_extensions.Literal["retrieval_time"] | None: ...

global___AuditRequest = AuditRequest
