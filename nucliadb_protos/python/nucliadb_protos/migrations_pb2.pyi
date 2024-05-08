"""
@generated by mypy-protobuf.  Do not edit manually!
isort:skip_file
"""
import builtins
import google.protobuf.descriptor
import google.protobuf.message
import sys

if sys.version_info >= (3, 8):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

@typing_extensions.final
class MigrationInfo(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    CURRENT_VERSION_FIELD_NUMBER: builtins.int
    TARGET_VERSION_FIELD_NUMBER: builtins.int
    current_version: builtins.int
    target_version: builtins.int
    def __init__(
        self,
        *,
        current_version: builtins.int = ...,
        target_version: builtins.int = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["current_version", b"current_version", "target_version", b"target_version"]) -> None: ...

global___MigrationInfo = MigrationInfo
