import traceback

from opentelemetry.sdk.trace import Span  # type: ignore
from opentelemetry.trace.status import Status, StatusCode  # type: ignore


def set_span_exception(span: Span, exception: Exception):
    description = traceback.format_exc()

    span.set_status(
        Status(
            status_code=StatusCode.ERROR,
            description=description,
        )
    )
    span.record_exception(exception)
