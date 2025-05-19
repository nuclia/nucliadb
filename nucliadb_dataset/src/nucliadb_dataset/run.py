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
from urllib.parse import urlparse

import argdantic

from nucliadb_dataset import ExportType
from nucliadb_dataset.dataset import TASK_DEFINITIONS
from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_dataset.settings import RunningSettings
from nucliadb_protos.dataset_pb2 import TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB

parser: argdantic.ArgParser = argdantic.ArgParser()


@parser.command(
    singleton=True,
    name="NucliaDB Dataset",
    help="Generate Arrow files from NucliaDB KBs",
)
def setting(settings: RunningSettings) -> RunningSettings:
    return settings


def run():
    nucliadb_args = parser()

    trainset = TrainSet()
    definition = TASK_DEFINITIONS[nucliadb_args.type]
    trainset.type = definition.proto
    trainset.batch_size = nucliadb_args.batch_size
    if definition.labels:
        if nucliadb_args.labelset is not None:
            trainset.filter.labels.append(nucliadb_args.labelset)

    nuclia_url_parts = urlparse(nucliadb_args.url)
    url = f"{nuclia_url_parts.scheme}://{nuclia_url_parts.netloc}/api"

    Path(nucliadb_args.download_path).mkdir(parents=True, exist_ok=True)
    if nucliadb_args.export == ExportType.DATASETS:
        if nucliadb_args.apikey is None:
            raise Exception("API key required to push to Nuclia Dataset™")
        sdk = NucliaDB(
            region=nucliadb_args.environment,
            url=url,
            api_key=nucliadb_args.service_token,
        )
        fse = NucliaDatasetsExport(
            sdk=sdk,
            kbid=nucliadb_args.kbid,
            datasets_url=nucliadb_args.datasets_url,
            trainset=trainset,
            cache_path=nucliadb_args.download_path,
            apikey=nucliadb_args.apikey,
        )
        fse.export()
    elif nucliadb_args.export == ExportType.FILESYSTEM:
        sdk = NucliaDB(
            region=nucliadb_args.environment,
            url=url,
            api_key=nucliadb_args.service_token,
        )
        fse = FileSystemExport(
            sdk=sdk,
            kbid=nucliadb_args.kbid,
            trainset=trainset,
            store_path=nucliadb_args.download_path,
        )
        fse.export()
