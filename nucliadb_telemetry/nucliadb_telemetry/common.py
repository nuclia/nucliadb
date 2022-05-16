import traceback
from typing import Optional

from opentelemetry.context import detach
from opentelemetry.sdk.trace import Span  # type: ignore
from opentelemetry.trace.status import Status, StatusCode  # type: ignore


def set_span_exception(span: Optional[Span], exception: Exception):
    if span is not None:
        description = traceback.format_exc()

        span.set_status(
            Status(
                status_code=StatusCode.ERROR,
                description=description,
            )
        )
        span.record_exception(exception)
        span.end()


def finish_span(span: Optional[Span]):
    if span is not None:
        span.end()
    if hasattr(span, "token"):
        detach(span.token)
