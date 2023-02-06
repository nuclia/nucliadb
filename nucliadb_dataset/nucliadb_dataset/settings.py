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

from pathlib import Path
from typing import List, Optional

import pydantic
from pydantic import BaseSettings

from nucliadb_dataset import DatasetType, ExportType
from nucliadb_sdk.client import Environment


class Settings(BaseSettings):
    train_grpc_address: str = "train.nucliadb.svc:8080"


settings = Settings()


class RunningSettings(pydantic.BaseSettings):
    export: ExportType = pydantic.Field(
        ExportType.FILESYSTEM, description="Destination of export"
    )
    download_path: str = pydantic.Field(
        f"{Path.home()}/.nuclia/download", description="Download path"
    )
    url: str = pydantic.Field(description="KnowledgeBox URL")
    type: DatasetType = pydantic.Field(description="Dataset Type")
    labelset: Optional[str] = pydantic.Field(
        description="For classification which labelset"
    )
    families: Optional[List[str]] = pydantic.Field(description="List of family group")

    datasets_url: Optional[str] = pydantic.Field(
        description="Base url for the Nuclia datasets component (including /api/v1)™"
    )

    apikey: Optional[str] = pydantic.Field(
        description="API key to upload to Nuclia Datasets™"
    )

    environment: Environment = pydantic.Field(
        Environment.OSS, description="CLOUD or OSS"
    )

    service_token: Optional[str] = pydantic.Field(
        description="Service account key to access Nuclia Cloud"
    )

    batch_size: int = pydantic.Field(64, description="Batch streaming size")
