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
