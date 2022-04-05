import typing
from opentelemetry.context.context import Context as Context
from opentelemetry import textmap
from typing import Any

logger: Any

def extract(
    carrier: textmap.CarrierT,
    context: typing.Optional[Context] = ...,
    getter: textmap.Getter = ...,
) -> Context: ...
def inject(
    carrier: textmap.CarrierT,
    context: typing.Optional[Context] = ...,
    setter: textmap.Setter = ...,
) -> None: ...

propagators: Any
environ_propagators: Any
propagator: Any

def get_global_textmap() -> textmap.TextMapPropagator: ...
def set_global_textmap(http_text_format: textmap.TextMapPropagator) -> None: ...
