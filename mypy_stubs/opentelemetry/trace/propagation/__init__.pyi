from opentelemetry.context.context import Context as Context
from opentelemetry.trace.span import Span
from typing import Optional

SPAN_KEY: str

def set_span_in_context(span: Span, context: Optional[Context] = ...) -> Context: ...
def get_current_span(context: Optional[Context] = ...) -> Span: ...
