# Copyright (C) 2021 Bosutech XXI S.L.
#
# nucliadb is offered under the AGPL v3.0 and as commercial software.
# For commercial licensing, contact us at info@nuclia.com.
#
# AGPL:
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as
# published by the Free Software Foundation, either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

# inspired from datadog integrations like https://pypi.org/project/JSON-log-formatter/
import logging
from copy import copy
from datetime import datetime
from typing import Any

import orjson
import pydantic
from opentelemetry import trace
from opentelemetry.trace import format_span_id, format_trace_id
from opentelemetry.trace.span import INVALID_SPAN

from nucliadb_telemetry.settings import LogLevel, LogSettings

from . import context

try:
    from uvicorn.logging import AccessFormatter  # type: ignore
except ImportError:  # pragma: no cover
    AccessFormatter = logging.Formatter

_BUILTIN_ATTRS = (
    # list of all possible args
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
)


class JSONFormatter(logging.Formatter):
    """
    Formatter base json-log-formatter with pydantic support
    """

    def format(self, record: logging.LogRecord) -> str:
        extra: dict[str, Any]
        if isinstance(record.msg, dict):
            extra = record.msg
        elif isinstance(record.msg, pydantic.BaseModel):
            extra = record.msg.dict()
        else:
            extra = {"message": record.getMessage()}
        extra.update(self.extra_from_record(record))

        self.fill_log_data(extra, record)

        return orjson.dumps(extra, default=repr).decode("utf-8", errors="ignore")

    def fill_log_data(self, data: dict[str, Any], record: logging.LogRecord) -> None:
        if "time" not in data:
            data["time"] = datetime.utcnow()

        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)

        data["filename"] = record.filename
        data["module"] = record.module
        data["name"] = record.name

        # GCP specific fields
        data["severity"] = record.levelname
        data["logging.googleapis.com/sourceLocation"] = {
            "file": record.pathname,
            "line": record.lineno,
            "function": record.funcName,
        }

        current_ctx = context.get_context()
        if len(current_ctx) > 0:
            data["context"] = current_ctx

        current_span = trace.get_current_span()
        if current_span not in (INVALID_SPAN, None):
            span_context = current_span.get_span_context()
            # for us, this is opentelemetry trace_id/span_id
            # GCP has logging.googleapis.com/spanId but it's for it's own cloud tracing system
            data["trace_id"] = format_trace_id(span_context.trace_id)
            data["span_id"] = format_span_id(span_context.span_id)

        if hasattr(record, "stack_info"):
            data["stack_info"] = record.stack_info
        else:  # pragma: no cover
            data["stack_info"] = None

    def extra_from_record(self, record):
        return {
            attr_name: record.__dict__[attr_name]
            for attr_name in record.__dict__
            if attr_name not in _BUILTIN_ATTRS
        }


class UvicornAccessFormatter(JSONFormatter):
    def format(self, record: logging.LogRecord) -> str:
        recordcopy = copy(record)
        (
            client_addr,
            method,
            full_path,
            http_version,
            status_code,
        ) = recordcopy.args  # type: ignore[misc]
        request_line = "%s %s HTTP/%s" % (method, full_path, http_version)
        recordcopy.__dict__.update(
            {
                "httpRequest": {
                    "requestMethod": method,
                    "requestUrl": full_path,
                    "status": status_code,
                    "remoteIp": client_addr,
                    "protocol": http_version,
                },
                "message": request_line,
            }
        )
        return super().format(recordcopy)


_ACCESS_LOGGER_NAME = "uvicorn.access"


_default_logger_levels = {
    # some are too chatty
    "uvicorn.error": LogLevel.WARNING,
    "nucliadb_utils.utilities": LogLevel.WARNING,
    # needed always for access logs
    _ACCESS_LOGGER_NAME: LogLevel.INFO,
}


def setup_logging() -> None:
    settings = LogSettings()

    if settings.logger_levels is None:
        settings.logger_levels = {}

    for logger_name, level in _default_logger_levels.items():
        if logger_name not in settings.logger_levels:
            settings.logger_levels[logger_name] = level

    formatter = JSONFormatter()
    json_handler = logging.StreamHandler()
    json_handler.setFormatter(formatter)
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(
        logging.Formatter(
            "[%(asctime)s.%(msecs)02d] [%(levelname)s] - %(name)s - %(message)s"
        )
    )

    root_logger = logging.getLogger()
    access_logger = logging.getLogger(_ACCESS_LOGGER_NAME)
    access_logger.handlers = []
    if not settings.debug:
        root_logger.addHandler(json_handler)
        access_handler = logging.StreamHandler()
        access_handler.setFormatter(UvicornAccessFormatter())
        access_logger.addHandler(access_handler)
    else:
        root_logger.addHandler(stream_handler)
        # regular stream access logs
        access_handler = logging.StreamHandler()
        access_handler.setFormatter(AccessFormatter())  # not json based
        access_logger.addHandler(access_handler)

    root_logger.setLevel(getattr(logging, settings.log_level.value))

    for logger_name, level in settings.logger_levels.items():
        log = logging.getLogger(logger_name)
        if logger_name != _ACCESS_LOGGER_NAME:
            if not settings.debug:
                log.addHandler(json_handler)
            else:
                root_logger.addHandler(stream_handler)

        log.propagate = False
        log.setLevel(getattr(logging, level.value))
