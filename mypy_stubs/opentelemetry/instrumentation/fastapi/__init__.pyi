import fastapi
from opentelemetry.instrumentor import BaseInstrumentor
from typing import Any, Collection

class FastAPIInstrumentor(BaseInstrumentor):
    @staticmethod
    def instrument_app(
        app: Any,
        server_request_hook: Any = ...,
        client_request_hook: Any = ...,
        client_response_hook: Any = ...,
        tracer_provider: Any | None = ...,
        excluded_urls: Any | None = ...,
    ): ...
    @staticmethod
    def uninstrument_app(app: Any): ...
    def instrumentation_dependencies(self) -> Collection[str]: ...

class _InstrumentedFastAPI(fastapi.FastAPI):
    def __init__(self, *args, **kwargs) -> None: ...
