# Copyright 2025 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import enum

import pydantic
from pydantic_settings import BaseSettings


class TelemetrySettings(BaseSettings):
    otlp_collector_endpoint: str | None = None

    observe_garbage_collector: bool = pydantic.Field(
        default=False, description="Instrument and observe GC metrics"
    )

    prometheus_request_duration_buckets: list[float] = pydantic.Field(
        default=[0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0],
        description=(
            "Histogram bucket boundaries (in seconds) for the "
            "starlette_requests_processing_time_seconds metric. "
            "Defaults to the standard Prometheus buckets (max 10 s). "
            "Set this to cover the full latency range of the service, e.g. "
            "[0.1, 0.5, 1, 2.5, 5, 10, 20, 30, 45, 60, 90, 120]"
        ),
    )

    log_requests_start: bool = pydantic.Field(
        default=False,
        description=(
            "Log an INFO entry when each request is received, including the "
            "HTTP method, path template, and trace ID (when tracing is active). "
            "FastAPI already logs requests upon completion; this flag adds the "
            "arrival log that is otherwise missing"
        ),
    )

    def tracing_enabled(self) -> bool:
        return self.otlp_collector_endpoint is not None


telemetry_settings = TelemetrySettings()


class LogLevel(enum.Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    FATAL = "FATAL"
    CRITICAL = "CRITICAL"


class LogOutputType(enum.Enum):
    STDOUT = "stdout"
    FILE = "file"


class LogFormatType(enum.Enum):
    PLAIN = "plain"
    STRUCTURED = "structured"


class LogSettings(BaseSettings):
    debug: bool = False
    log_level: LogLevel = LogLevel.WARNING
    logger_levels: dict[str, LogLevel] | None = None
    log_output_type: LogOutputType = LogOutputType.STDOUT
    log_format_type: LogFormatType = LogFormatType.STRUCTURED

    access_log: str = pydantic.Field(
        default="logs/access.log",
        description="If using file log output, this is the path to the access log file.",
    )
    info_log: str = pydantic.Field(
        default="logs/info.log",
        description="If using file log output, this is the path to all logs that are not error or access logs.",
    )
    error_log: str = pydantic.Field(
        default="logs/error.log",
        description="If using file log output, this is the path to the error log file.",
    )
    max_log_file_size: int = pydantic.Field(
        default=1024 * 1024 * 50,
        description="Max file size in bytes before rotating log file.",
    )
