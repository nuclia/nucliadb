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
from nucliadb_dataset import DatasetType, ExportType
from nucliadb_protos.train_pb2 import Type
from nucliadb_sdk.client import Environment
from pydantic import BaseSettings
from enum import Enum
from typing import List, Optional

import pydantic


class Settings(BaseSettings):

    train_grpc_address: str = "train.nucliadb.svc:8080"


settings = Settings()


class RunningSettings(pydantic.BaseSettings):
    url: str = pydantic.Field(description="KnowledgeBox URL")
    type: DatasetType = pydantic.Field(description="Dataset Type")
    labelset: Optional[str] = pydantic.Field(
        description="For classification which labelset"
    )
    family: Optional[List[str]] = pydantic.Field(description="List of family group")

    export: ExportType = pydantic.Field(description="Destination of export")
