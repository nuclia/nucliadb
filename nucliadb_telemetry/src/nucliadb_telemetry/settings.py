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
from typing import Dict, Optional

import pydantic
from pydantic_settings import BaseSettings


class TelemetrySettings(BaseSettings):
    jaeger_agent_host: str = "localhost"
    jaeger_agent_port: int = 6831
    jaeger_enabled: bool = False
    jaeger_query_port: int = 16686
    jaeger_query_host: str = "jaeger.observability.svc.cluster.local"


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
    logger_levels: Optional[Dict[str, LogLevel]] = None
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
