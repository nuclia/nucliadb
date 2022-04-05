import abc
import typing
from abc import ABC, abstractmethod
from enum import Enum
from opentelemetry.context.context import Context
from opentelemetry.trace.propagation import (
    get_current_span as get_current_span,
    set_span_in_context as set_span_in_context,
)
from opentelemetry.trace.span import (
    DEFAULT_TRACE_OPTIONS as DEFAULT_TRACE_OPTIONS,
    DEFAULT_TRACE_STATE as DEFAULT_TRACE_STATE,
    INVALID_SPAN as INVALID_SPAN,
    INVALID_SPAN_CONTEXT as INVALID_SPAN_CONTEXT,
    INVALID_SPAN_ID as INVALID_SPAN_ID,
    INVALID_TRACE_ID as INVALID_TRACE_ID,
    NonRecordingSpan as NonRecordingSpan,
    Span as Span,
    SpanContext as SpanContext,
    TraceFlags as TraceFlags,
    TraceState as TraceState,
    format_span_id as format_span_id,
    format_trace_id as format_trace_id,
)
from opentelemetry.trace.status import Status as Status, StatusCode as StatusCode
from opentelemetry import types
from typing import Any, Iterator, Optional

class _LinkBase(ABC, metaclass=abc.ABCMeta):
    def __init__(self, context: SpanContext) -> None: ...
    @property
    def context(self) -> SpanContext: ...
    @property
    @abstractmethod
    def attributes(self) -> types.Attributes: ...

class Link(_LinkBase):
    def __init__(
        self, context: SpanContext, attributes: types.Attributes = ...
    ) -> None: ...
    @property
    def attributes(self) -> types.Attributes: ...

class SpanKind(Enum):
    INTERNAL: int
    SERVER: int
    CLIENT: int
    PRODUCER: int
    CONSUMER: int

class TracerProvider(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: typing.Optional[str] = ...,
        schema_url: typing.Optional[str] = ...,
    ) -> Tracer: ...

class _DefaultTracerProvider(TracerProvider):
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: typing.Optional[str] = ...,
        schema_url: typing.Optional[str] = ...,
    ) -> Tracer: ...

class ProxyTracerProvider(TracerProvider):
    def get_tracer(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: typing.Optional[str] = ...,
        schema_url: typing.Optional[str] = ...,
    ) -> Tracer: ...

class Tracer(ABC, metaclass=abc.ABCMeta):
    @abstractmethod
    def start_span(
        self,
        name: str,
        context: Optional[Context] = ...,
        kind: SpanKind = ...,
        attributes: types.Attributes = ...,
        links: Any = ...,
        start_time: Optional[int] = ...,
        record_exception: bool = ...,
        set_status_on_exception: bool = ...,
    ) -> Span: ...

class ProxyTracer(Tracer):
    def __init__(
        self,
        instrumenting_module_name: str,
        instrumenting_library_version: typing.Optional[str] = ...,
        schema_url: typing.Optional[str] = ...,
    ) -> None: ...
    def start_span(self, *args, **kwargs) -> Span: ...
    def start_as_current_span(self, *args, **kwargs) -> Span: ...

class _DefaultTracer(Tracer):
    def start_span(
        self,
        name: str,
        context: Optional[Context] = ...,
        kind: SpanKind = ...,
        attributes: types.Attributes = ...,
        links: Any = ...,
        start_time: Optional[int] = ...,
        record_exception: bool = ...,
        set_status_on_exception: bool = ...,
    ) -> Span: ...
    def start_as_current_span(
        self,
        name: str,
        context: Optional[Context] = ...,
        kind: SpanKind = ...,
        attributes: types.Attributes = ...,
        links: Any = ...,
        start_time: Optional[int] = ...,
        record_exception: bool = ...,
        set_status_on_exception: bool = ...,
        end_on_exit: bool = ...,
    ) -> Iterator["Span"]: ...

def get_tracer(
    instrumenting_module_name: str,
    instrumenting_library_version: typing.Optional[str] = ...,
    tracer_provider: Optional[TracerProvider] = ...,
    schema_url: typing.Optional[str] = ...,
) -> Tracer: ...
def set_tracer_provider(tracer_provider: TracerProvider) -> None: ...
def get_tracer_provider() -> TracerProvider: ...
def use_span(
    span: Span,
    end_on_exit: bool = ...,
    record_exception: bool = ...,
    set_status_on_exception: bool = ...,
) -> Iterator[Span]: ...
