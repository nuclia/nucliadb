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
