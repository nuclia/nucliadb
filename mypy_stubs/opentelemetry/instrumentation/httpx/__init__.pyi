import httpx
import typing
from opentelemetry.instrumentor import BaseInstrumentor
from opentelemetry.trace import TracerProvider as TracerProvider
from typing import Any

URL: Any
Headers: Any
RequestHook: Any
ResponseHook: Any
AsyncRequestHook: Any
AsyncResponseHook: Any

class RequestInfo(typing.NamedTuple):
    method: bytes
    url: URL
    headers: typing.Optional[Headers]
    stream: typing.Optional[typing.Union[httpx.SyncByteStream, httpx.AsyncByteStream]]
    extensions: typing.Optional[dict]

class ResponseInfo(typing.NamedTuple):
    status_code: int
    headers: typing.Optional[Headers]
    stream: typing.Iterable[bytes]
    extensions: typing.Optional[dict]

class SyncOpenTelemetryTransport(httpx.BaseTransport):
    def __init__(
        self,
        transport: httpx.BaseTransport,
        tracer_provider: typing.Optional[TracerProvider] = ...,
        request_hook: typing.Optional[RequestHook] = ...,
        response_hook: typing.Optional[ResponseHook] = ...,
    ) -> None: ...

class AsyncOpenTelemetryTransport(httpx.AsyncBaseTransport):
    def __init__(
        self,
        transport: httpx.AsyncBaseTransport,
        tracer_provider: typing.Optional[TracerProvider] = ...,
        request_hook: typing.Optional[RequestHook] = ...,
        response_hook: typing.Optional[ResponseHook] = ...,
    ) -> None: ...

class _InstrumentedClient(httpx.Client):
    def __init__(self, *args, **kwargs) -> None: ...

class _InstrumentedAsyncClient(httpx.AsyncClient):
    def __init__(self, *args, **kwargs) -> None: ...

class HTTPXClientInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> typing.Collection[str]: ...
    @staticmethod
    def instrument_client(
        client: typing.Union[httpx.Client, httpx.AsyncClient],
        tracer_provider: TracerProvider = ...,
        request_hook: typing.Optional[RequestHook] = ...,
        response_hook: typing.Optional[ResponseHook] = ...,
    ) -> None: ...
    @staticmethod
    def uninstrument_client(client: typing.Union[httpx.Client, httpx.AsyncClient]): ...
