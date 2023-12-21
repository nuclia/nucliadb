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
import os
from copy import copy
from datetime import datetime
from logging.handlers import RotatingFileHandler
from typing import Any, Dict, Optional

import orjson
import pydantic
from opentelemetry import trace
from opentelemetry.trace import format_span_id, format_trace_id
from opentelemetry.trace.span import INVALID_SPAN

from nucliadb_telemetry.settings import (
    LogFormatType,
    LogLevel,
    LogOutputType,
    LogSettings,
)

from . import context

try:
    from uvicorn.logging import AccessFormatter  # type: ignore
except ImportError:  # pragma: no cover
    AccessFormatter = logging.Formatter

_BUILTIN_ATTRS = set(
    [
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
    ]
)


ACCESS_LOG_FMT = (
    "%(asctime)s.%(msecs)03d - %(client_addr)s - %(request_line)s %(status_code)s"
)
ACCESS_LOG_DATEFMT = "%Y-%m-%d,%H:%M:%S"


def extra_from_record(record) -> Dict[str, Any]:
    return {
        attr_name: record.__dict__[attr_name]
        for attr_name in set(record.__dict__) - _BUILTIN_ATTRS
    }


class JSONFormatter(logging.Formatter):
    """
    Formatter base json-log-formatter with pydantic support
    """

    def format(self, record: logging.LogRecord) -> str:
        extra: Dict[str, Any]
        if isinstance(record.msg, dict):
            extra = record.msg
        elif isinstance(record.msg, pydantic.BaseModel):
            extra = record.msg.dict()
        else:
            extra = {"message": record.getMessage()}
        extra.update(extra_from_record(record))

        self.fill_log_data(extra, record)

        return orjson.dumps(extra, default=repr).decode("utf-8", errors="ignore")

    def fill_log_data(self, data: Dict[str, Any], record: logging.LogRecord) -> None:
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


class ExtraFormatter(logging.Formatter):
    def format(self, record):
        if not hasattr(record, "extra_formatted"):
            # format all extra fields as a comma separated list
            extra_fmted = " -- " + ", ".join(
                [f"{k}={v}" for k, v in extra_from_record(record).items()]
            )
            setattr(record, "extra_formatted", extra_fmted)
        return super().format(record)


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
}


def _maybe_create_file_directory(path: str) -> None:
    directory = os.path.dirname(path)
    os.makedirs(directory, exist_ok=True)


def setup_access_logging(settings: LogSettings) -> None:
    # setup access logger uniquely
    access_logger = logging.getLogger(_ACCESS_LOGGER_NAME)
    access_logger.handlers = []
    access_logger.propagate = False
    access_logger.setLevel(logging.INFO)

    access_handler: logging.Handler
    if settings.log_output_type == LogOutputType.FILE:
        _maybe_create_file_directory(settings.access_log)
        access_handler = RotatingFileHandler(
            settings.access_log, maxBytes=settings.max_log_file_size, backupCount=5
        )
    else:
        access_handler = logging.StreamHandler()
    access_logger.addHandler(access_handler)

    if not settings.debug and settings.log_format_type == LogFormatType.STRUCTURED:
        access_handler.setFormatter(
            UvicornAccessFormatter(
                fmt=ACCESS_LOG_FMT,
                datefmt=ACCESS_LOG_DATEFMT,
            )
        )
    else:
        # regular stream access logs
        access_handler.setFormatter(
            AccessFormatter(  # not json based
                fmt=ACCESS_LOG_FMT,
                datefmt=ACCESS_LOG_DATEFMT,
            )
        )


def setup_logging(*, settings: Optional[LogSettings] = None) -> None:
    if settings is None:
        settings = LogSettings()

    if settings.logger_levels is None:
        settings.logger_levels = {}

    for logger_name, level in _default_logger_levels.items():
        if logger_name not in settings.logger_levels:
            settings.logger_levels[logger_name] = level

    log_handler: logging.Handler
    error_log_handler: logging.Handler
    if settings.log_output_type == LogOutputType.FILE:
        _maybe_create_file_directory(settings.info_log)
        _maybe_create_file_directory(settings.error_log)
        log_handler = RotatingFileHandler(
            settings.info_log, maxBytes=settings.max_log_file_size, backupCount=5
        )
        error_log_handler = RotatingFileHandler(
            settings.error_log, maxBytes=settings.max_log_file_size, backupCount=5
        )
        error_log_handler.setLevel(logging.ERROR)
    else:
        # we only use error_log_handler for file output so this
        # ends up not being used
        log_handler = error_log_handler = logging.StreamHandler()

    if not settings.debug and settings.log_format_type == LogFormatType.STRUCTURED:
        log_handler.setFormatter(JSONFormatter())
        error_log_handler.setFormatter(JSONFormatter())
    else:
        formatter = ExtraFormatter(
            "[%(asctime)s.%(msecs)02d][%(levelname)s][%(name)s] - %(message)s%(extra_formatted)s"
        )
        log_handler.setFormatter(formatter)
        error_log_handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, settings.log_level.value))
    root_logger.addHandler(log_handler)
    if settings.log_output_type == LogOutputType.FILE:
        # error log handler is only use for file output
        root_logger.addHandler(error_log_handler)

    setup_access_logging(settings)

    # customized log levels
    for logger_name, level in settings.logger_levels.items():
        log = logging.getLogger(logger_name)
        log.addHandler(log_handler)
        if settings.log_output_type == LogOutputType.FILE:
            log.addHandler(error_log_handler)

        log.propagate = False
        log.setLevel(getattr(logging, level.value))
