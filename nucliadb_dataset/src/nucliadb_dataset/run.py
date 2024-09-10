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
