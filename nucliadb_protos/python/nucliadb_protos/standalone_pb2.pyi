"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""

import builtins
import google.protobuf.descriptor
import google.protobuf.message
import typing

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing.final
class NodeActionRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    SERVICE_FIELD_NUMBER: builtins.int
    ACTION_FIELD_NUMBER: builtins.int
    PAYLOAD_FIELD_NUMBER: builtins.int
    service: builtins.str
    action: builtins.str
    payload: builtins.bytes
    def __init__(
        self,
        *,
        service: builtins.str = ...,
        action: builtins.str = ...,
        payload: builtins.bytes = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing.Literal["action", b"action", "payload", b"payload", "service", b"service"]) -> None: ...

global___NodeActionRequest = NodeActionRequest

@typing.final
class NodeActionResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    PAYLOAD_FIELD_NUMBER: builtins.int
    payload: builtins.bytes
    def __init__(
        self,
        *,
        payload: builtins.bytes = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing.Literal["payload", b"payload"]) -> None: ...

global___NodeActionResponse = NodeActionResponse

@typing.final
class NodeInfoRequest(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    def __init__(
        self,
    ) -> None: ...

global___NodeInfoRequest = NodeInfoRequest

@typing.final
class NodeInfoResponse(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    ID_FIELD_NUMBER: builtins.int
    ADDRESS_FIELD_NUMBER: builtins.int
    SHARD_COUNT_FIELD_NUMBER: builtins.int
    AVAILABLE_DISK_FIELD_NUMBER: builtins.int
    TOTAL_DISK_FIELD_NUMBER: builtins.int
    id: builtins.str
    address: builtins.str
    shard_count: builtins.int
    available_disk: builtins.int
    total_disk: builtins.int
    def __init__(
        self,
        *,
        id: builtins.str = ...,
        address: builtins.str = ...,
        shard_count: builtins.int = ...,
        available_disk: builtins.int = ...,
        total_disk: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing.Literal["address", b"address", "available_disk", b"available_disk", "id", b"id", "shard_count", b"shard_count", "total_disk", b"total_disk"]) -> None: ...

global___NodeInfoResponse = NodeInfoResponse
