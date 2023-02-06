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
#
from enum import Enum
from typing import Optional

import pydantic


class LogLevel(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class Driver(str, Enum):
    REDIS = "REDIS"
    LOCAL = "LOCAL"


class Settings(pydantic.BaseSettings):
    driver: Driver = pydantic.Field(Driver.LOCAL, description="Main DB Path string")
    maindb: str = pydantic.Field("./data/main", description="Main DB Path string")
    blob: str = pydantic.Field("./data/blob", description="Blob Path string")
    key: Optional[str] = pydantic.Field(
        description="Nuclia Understanding API Key string"
    )
    node: str = pydantic.Field("./data/node", description="Node Path string")
    zone: Optional[str] = pydantic.Field(description="Nuclia Understanding API Zone ID")
    http: int = pydantic.Field(8080, description="HTTP Port int")
    grpc: int = pydantic.Field(8030, description="GRPC Port int")
    train: int = pydantic.Field(8031, description="Train GRPC Port int")
    log: LogLevel = pydantic.Field(
        LogLevel.INFO, description="Log level [DEBUG,INFO,ERROR] string"
    )
