import traceback
from typing import Optional

from opentelemetry.context import detach
from opentelemetry.sdk.trace import Span  # type: ignore
from opentelemetry.trace.status import Status, StatusCode  # type: ignore

from nucliadb_telemetry import logger


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
        if span._end_time is not None:
            logger.error("Aqui 1")
        span.end()


def finish_span(span: Optional[Span]):
    if span is not None:
        if span._end_time is not None:
            logger.error("Aqui 2")
        span.end()
    if hasattr(span, "token"):
        detach(span.token)
