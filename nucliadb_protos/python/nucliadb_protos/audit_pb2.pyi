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
import nucliadb_protos.nodereader_pb2
import nucliadb_protos.resources_pb2
import typing
import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _ClientType:
    ValueType = typing.NewType('ValueType', builtins.int)
    V: typing_extensions.TypeAlias = ValueType
class _ClientTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_ClientType.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    API: _ClientType.ValueType  # 0
    WEB: _ClientType.ValueType  # 1
    WIDGET: _ClientType.ValueType  # 2
    DESKTOP: _ClientType.ValueType  # 3
    DASHBOARD: _ClientType.ValueType  # 4
    CHROME_EXTENSION: _ClientType.ValueType  # 5
class ClientType(_ClientType, metaclass=_ClientTypeEnumTypeWrapper):
    pass

API: ClientType.ValueType  # 0
WEB: ClientType.ValueType  # 1
WIDGET: ClientType.ValueType  # 2
DESKTOP: ClientType.ValueType  # 3
DASHBOARD: ClientType.ValueType  # 4
CHROME_EXTENSION: ClientType.ValueType  # 5
global___ClientType = ClientType


class AuditField(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _FieldAction:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _FieldActionEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[AuditField._FieldAction.ValueType], builtins.type):
        DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
        ADDED: AuditField._FieldAction.ValueType  # 0
        MODIFIED: AuditField._FieldAction.ValueType  # 1
        DELETED: AuditField._FieldAction.ValueType  # 2
    class FieldAction(_FieldAction, metaclass=_FieldActionEnumTypeWrapper):
        pass

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
    field_id: typing.Text
    field_type: nucliadb_protos.resources_pb2.FieldType.ValueType
    filename: typing.Text
    def __init__(self,
        *,
        action: global___AuditField.FieldAction.ValueType = ...,
        size: builtins.int = ...,
        size_delta: builtins.int = ...,
        field_id: typing.Text = ...,
        field_type: nucliadb_protos.resources_pb2.FieldType.ValueType = ...,
        filename: typing.Text = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["action",b"action","field_id",b"field_id","field_type",b"field_type","filename",b"filename","size",b"size","size_delta",b"size_delta"]) -> None: ...
global___AuditField = AuditField

class AuditShardCounter(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    SHARD_FIELD_NUMBER: builtins.int
    PARAGRAPHS_FIELD_NUMBER: builtins.int
    FIELDS_FIELD_NUMBER: builtins.int
    shard: typing.Text
    paragraphs: builtins.int
    fields: builtins.int
    def __init__(self,
        *,
        shard: typing.Text = ...,
        paragraphs: builtins.int = ...,
        fields: builtins.int = ...,
        ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["fields",b"fields","paragraphs",b"paragraphs","shard",b"shard"]) -> None: ...
global___AuditShardCounter = AuditShardCounter

class AuditRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor
    class _AuditType:
        ValueType = typing.NewType('ValueType', builtins.int)
        V: typing_extensions.TypeAlias = ValueType
    class _AuditTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[AuditRequest._AuditType.ValueType], builtins.type):
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
    class AuditType(_AuditType, metaclass=_AuditTypeEnumTypeWrapper):
        pass

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
    COUNTER_FIELD_NUMBER: builtins.int
    CLIENT_TYPE_FIELD_NUMBER: builtins.int
    TRACE_ID_FIELD_NUMBER: builtins.int
    type: global___AuditRequest.AuditType.ValueType
    kbid: typing.Text
    userid: typing.Text
    @property
    def time(self) -> google.protobuf.timestamp_pb2.Timestamp: ...
    @property
    def fields(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[typing.Text]: ...
    @property
    def search(self) -> nucliadb_protos.nodereader_pb2.SearchRequest: ...
    timeit: builtins.float
    origin: typing.Text
    rid: typing.Text
    task: typing.Text
    resources: builtins.int
    @property
    def field_metadata(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[nucliadb_protos.resources_pb2.FieldID]: ...
    @property
    def fields_audit(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___AuditField]: ...
    @property
    def counter(self) -> global___AuditShardCounter: ...
    client_type: global___ClientType.ValueType
    trace_id: typing.Text
    def __init__(self,
        *,
        type: global___AuditRequest.AuditType.ValueType = ...,
        kbid: typing.Text = ...,
        userid: typing.Text = ...,
        time: typing.Optional[google.protobuf.timestamp_pb2.Timestamp] = ...,
        fields: typing.Optional[typing.Iterable[typing.Text]] = ...,
        search: typing.Optional[nucliadb_protos.nodereader_pb2.SearchRequest] = ...,
        timeit: builtins.float = ...,
        origin: typing.Text = ...,
        rid: typing.Text = ...,
        task: typing.Text = ...,
        resources: builtins.int = ...,
        field_metadata: typing.Optional[typing.Iterable[nucliadb_protos.resources_pb2.FieldID]] = ...,
        fields_audit: typing.Optional[typing.Iterable[global___AuditField]] = ...,
        counter: typing.Optional[global___AuditShardCounter] = ...,
        client_type: global___ClientType.ValueType = ...,
        trace_id: typing.Text = ...,
        ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["counter",b"counter","search",b"search","time",b"time"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["client_type",b"client_type","counter",b"counter","field_metadata",b"field_metadata","fields",b"fields","fields_audit",b"fields_audit","kbid",b"kbid","origin",b"origin","resources",b"resources","rid",b"rid","search",b"search","task",b"task","time",b"time","timeit",b"timeit","trace_id",b"trace_id","type",b"type","userid",b"userid"]) -> None: ...
global___AuditRequest = AuditRequest
