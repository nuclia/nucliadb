import aiohttp
from opentelemetry.instrumentation.instrumentor import BaseInstrumentor
from opentelemetry.trace import TracerProvider as TracerProvider
from typing import Collection
import typing
import yarl
from opentelemetry.trace import Span

_UrlFilterT = typing.Optional[typing.Callable[[yarl.URL], str]]
_RequestHookT = typing.Optional[
    typing.Callable[[Span, aiohttp.TraceRequestStartParams], None]
]
_ResponseHookT = typing.Optional[
    typing.Callable[
        [
            Span,
            typing.Union[
                aiohttp.TraceRequestEndParams,
                aiohttp.TraceRequestExceptionParams,
            ],
        ],
        None,
    ]
]

def create_trace_config(
    url_filter: _UrlFilterT = ...,
    request_hook: _RequestHookT = ...,
    response_hook: _ResponseHookT = ...,
    tracer_provider: TracerProvider = ...,
) -> aiohttp.TraceConfig: ...

class AioHttpClientInstrumentor(BaseInstrumentor):
    def instrumentation_dependencies(self) -> Collection[str]: ...
    @staticmethod
    def uninstrument_session(client_session: aiohttp.ClientSession): ...
