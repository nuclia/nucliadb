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
import sys
import typing

if sys.version_info >= (3, 10):
    import typing as typing_extensions
else:
    import typing_extensions

DESCRIPTOR: google.protobuf.descriptor.FileDescriptor

class _TaskType:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _TaskTypeEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_TaskType.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    FIELD_CLASSIFICATION: _TaskType.ValueType  # 0
    PARAGRAPH_CLASSIFICATION: _TaskType.ValueType  # 1
    SENTENCE_CLASSIFICATION: _TaskType.ValueType  # 2
    TOKEN_CLASSIFICATION: _TaskType.ValueType  # 3

class TaskType(_TaskType, metaclass=_TaskTypeEnumTypeWrapper):
    """Train API V2"""

FIELD_CLASSIFICATION: TaskType.ValueType  # 0
PARAGRAPH_CLASSIFICATION: TaskType.ValueType  # 1
SENTENCE_CLASSIFICATION: TaskType.ValueType  # 2
TOKEN_CLASSIFICATION: TaskType.ValueType  # 3
global___TaskType = TaskType

class _LabelFrom:
    ValueType = typing.NewType("ValueType", builtins.int)
    V: typing_extensions.TypeAlias = ValueType

class _LabelFromEnumTypeWrapper(google.protobuf.internal.enum_type_wrapper._EnumTypeWrapper[_LabelFrom.ValueType], builtins.type):
    DESCRIPTOR: google.protobuf.descriptor.EnumDescriptor
    PARAGRAPH: _LabelFrom.ValueType  # 0
    FIELD: _LabelFrom.ValueType  # 1
    RESOURCE: _LabelFrom.ValueType  # 2

class LabelFrom(_LabelFrom, metaclass=_LabelFromEnumTypeWrapper): ...

PARAGRAPH: LabelFrom.ValueType  # 0
FIELD: LabelFrom.ValueType  # 1
RESOURCE: LabelFrom.ValueType  # 2
global___LabelFrom = LabelFrom

@typing_extensions.final
class TrainSet(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    @typing_extensions.final
    class Filter(google.protobuf.message.Message):
        DESCRIPTOR: google.protobuf.descriptor.Descriptor

        LABELS_FIELD_NUMBER: builtins.int
        @property
        def labels(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
        def __init__(
            self,
            *,
            labels: collections.abc.Iterable[builtins.str] | None = ...,
        ) -> None: ...
        def ClearField(self, field_name: typing_extensions.Literal["labels", b"labels"]) -> None: ...

    TYPE_FIELD_NUMBER: builtins.int
    FILTER_FIELD_NUMBER: builtins.int
    BATCH_SIZE_FIELD_NUMBER: builtins.int
    type: global___TaskType.ValueType
    @property
    def filter(self) -> global___TrainSet.Filter: ...
    batch_size: builtins.int
    def __init__(
        self,
        *,
        type: global___TaskType.ValueType = ...,
        filter: global___TrainSet.Filter | None = ...,
        batch_size: builtins.int = ...,
    ) -> None: ...
    def HasField(self, field_name: typing_extensions.Literal["filter", b"filter"]) -> builtins.bool: ...
    def ClearField(self, field_name: typing_extensions.Literal["batch_size", b"batch_size", "filter", b"filter", "type", b"type"]) -> None: ...

global___TrainSet = TrainSet

@typing_extensions.final
class Label(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    LABELSET_FIELD_NUMBER: builtins.int
    LABEL_FIELD_NUMBER: builtins.int
    ORIGIN_FIELD_NUMBER: builtins.int
    labelset: builtins.str
    label: builtins.str
    origin: global___LabelFrom.ValueType
    def __init__(
        self,
        *,
        labelset: builtins.str = ...,
        label: builtins.str = ...,
        origin: global___LabelFrom.ValueType = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["label", b"label", "labelset", b"labelset", "origin", b"origin"]) -> None: ...

global___Label = Label

@typing_extensions.final
class TextLabel(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TEXT_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    text: builtins.str
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Label]: ...
    def __init__(
        self,
        *,
        text: builtins.str = ...,
        labels: collections.abc.Iterable[global___Label] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["labels", b"labels", "text", b"text"]) -> None: ...

global___TextLabel = TextLabel

@typing_extensions.final
class MultipleTextSameLabels(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TEXT_FIELD_NUMBER: builtins.int
    LABELS_FIELD_NUMBER: builtins.int
    @property
    def text(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def labels(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___Label]: ...
    def __init__(
        self,
        *,
        text: collections.abc.Iterable[builtins.str] | None = ...,
        labels: collections.abc.Iterable[global___Label] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["labels", b"labels", "text", b"text"]) -> None: ...

global___MultipleTextSameLabels = MultipleTextSameLabels

@typing_extensions.final
class FieldClassificationBatch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATA_FIELD_NUMBER: builtins.int
    @property
    def data(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___TextLabel]: ...
    def __init__(
        self,
        *,
        data: collections.abc.Iterable[global___TextLabel] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["data", b"data"]) -> None: ...

global___FieldClassificationBatch = FieldClassificationBatch

@typing_extensions.final
class ParagraphClassificationBatch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATA_FIELD_NUMBER: builtins.int
    @property
    def data(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___TextLabel]: ...
    def __init__(
        self,
        *,
        data: collections.abc.Iterable[global___TextLabel] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["data", b"data"]) -> None: ...

global___ParagraphClassificationBatch = ParagraphClassificationBatch

@typing_extensions.final
class SentenceClassificationBatch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATA_FIELD_NUMBER: builtins.int
    @property
    def data(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___MultipleTextSameLabels]: ...
    def __init__(
        self,
        *,
        data: collections.abc.Iterable[global___MultipleTextSameLabels] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["data", b"data"]) -> None: ...

global___SentenceClassificationBatch = SentenceClassificationBatch

@typing_extensions.final
class TokensClassification(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    TOKEN_FIELD_NUMBER: builtins.int
    LABEL_FIELD_NUMBER: builtins.int
    @property
    def token(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    @property
    def label(self) -> google.protobuf.internal.containers.RepeatedScalarFieldContainer[builtins.str]: ...
    def __init__(
        self,
        *,
        token: collections.abc.Iterable[builtins.str] | None = ...,
        label: collections.abc.Iterable[builtins.str] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["label", b"label", "token", b"token"]) -> None: ...

global___TokensClassification = TokensClassification

@typing_extensions.final
class TokenClassificationBatch(google.protobuf.message.Message):
    DESCRIPTOR: google.protobuf.descriptor.Descriptor

    DATA_FIELD_NUMBER: builtins.int
    @property
    def data(self) -> google.protobuf.internal.containers.RepeatedCompositeFieldContainer[global___TokensClassification]: ...
    def __init__(
        self,
        *,
        data: collections.abc.Iterable[global___TokensClassification] | None = ...,
    ) -> None: ...
    def ClearField(self, field_name: typing_extensions.Literal["data", b"data"]) -> None: ...

global___TokenClassificationBatch = TokenClassificationBatch
