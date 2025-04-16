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
from typing import Optional

import pydantic
from pydantic_settings import BaseSettings

from nucliadb_dataset import ExportType
from nucliadb_dataset.tasks import Task


class Settings(BaseSettings):
    train_grpc_address: str = "train.nucliadb.svc:8080"


settings = Settings()


class RunningSettings(BaseSettings):
    export: ExportType = pydantic.Field(ExportType.FILESYSTEM, description="Destination of export")
    download_path: str = pydantic.Field(f"{Path.home()}/.nuclia/download", description="Download path")
    url: str = pydantic.Field(description="KnowledgeBox URL")
    type: Task = pydantic.Field(description="Dataset Type")
    labelset: Optional[str] = pydantic.Field(
        None, description="For classification which labelset or families"
    )

    datasets_url: str = pydantic.Field(
        "https://europe-1.nuclia.cloud",
        description="Base url for the Nuclia datasets component (excluding /api/v1)™",  # noqa
    )

    apikey: Optional[str] = pydantic.Field(None, description="API key to upload to Nuclia Datasets™")

    environment: str = pydantic.Field("on-prem", description="region or on-prem")

    service_token: Optional[str] = pydantic.Field(
        None, description="Service account key to access Nuclia Cloud"
    )

    batch_size: int = pydantic.Field(64, description="Batch streaming size")

    kbid: str = pydantic.Field(description="Knowledge Box UUID")
