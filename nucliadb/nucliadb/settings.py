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

from nucliadb.ingest.settings import DriverSettings
from nucliadb_utils.settings import StorageSettings


class LogLevel(str, Enum):
    INFO = "INFO"
    ERROR = "ERROR"
    DEBUG = "DEBUG"


class Settings(DriverSettings, StorageSettings):
    # be consistent here with DATA_PATH env var
    data_path: str = pydantic.Field(
        "./data/node", description="Path to node index files"
    )

    # all settings here are mapped in to other env var settings used
    # in the app. These are helper settings to make things easier to
    # use with standalone app vs cluster app.
    nua_api_key: Optional[str] = pydantic.Field(
        description="Nuclia Understanding API Key"
    )
    zone: Optional[str] = pydantic.Field(description="Nuclia Understanding API Zone ID")
    http_port: int = pydantic.Field(8080, description="HTTP Port")
    ingest_grpc_port: int = pydantic.Field(8030, description="Ingest GRPC Port int")
    train_grpc_port: int = pydantic.Field(8031, description="Train GRPC Port int")
