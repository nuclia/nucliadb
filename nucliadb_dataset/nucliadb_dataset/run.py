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

import pydantic_argparse
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset import DatasetType, ExportType
from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_sdk.client import NucliaDBClient

DATASET_TYPE_MAPPING = {
    DatasetType.FIELD_CLASSIFICATION: TaskType.FIELD_CLASSIFICATION,
    DatasetType.IMAGE_CLASSIFICATION: TaskType.IMAGE_CLASSIFICATION,
    DatasetType.PARAGRAPH_CLASSIFICATION: TaskType.PARAGRAPH_CLASSIFICATION,
    DatasetType.PARAGRAPH_STREAMING: TaskType.PARAGRAPH_STREAMING,
    DatasetType.QUESTION_ANSWER_STREAMING: TaskType.QUESTION_ANSWER_STREAMING,
    DatasetType.SENTENCE_CLASSIFICATION: TaskType.SENTENCE_CLASSIFICATION,
    DatasetType.TOKEN_CLASSIFICATION: TaskType.TOKEN_CLASSIFICATION,
}


def run():
    from nucliadb_dataset.settings import RunningSettings

    parser = pydantic_argparse.ArgumentParser(
        model=RunningSettings,
        prog="NucliaDB Datasets",
        description="Generate Arrow files from NucliaDB KBs",
    )
    nucliadb_args = parser.parse_typed_args()
    errors = []

    trainset = TrainSet()
    trainset.type = DATASET_TYPE_MAPPING[nucliadb_args.type]
    trainset.batch_size = nucliadb_args.batch_size
    if nucliadb_args.type in (
        DatasetType.FIELD_CLASSIFICATION,
        DatasetType.PARAGRAPH_CLASSIFICATION,
        DatasetType.SENTENCE_CLASSIFICATION,
    ):
        if nucliadb_args.labelset is not None:
            trainset.filter.labels.append(nucliadb_args.labelset)
    elif nucliadb_args.type in (DatasetType.TOKEN_CLASSIFICATION):
        if nucliadb_args.families is not None:
            trainset.filter.labels.extend(nucliadb_args.families)

    Path(nucliadb_args.download_path).mkdir(parents=True, exist_ok=True)
    if nucliadb_args.export == ExportType.DATASETS:
        if nucliadb_args.apikey is None:
            errors.append("API key required to push to Nuclia Dataset™")
        client = NucliaDBClient(
            environment=nucliadb_args.environment,
            url=nucliadb_args.url,
            api_key=nucliadb_args.service_token,
        )
        fse = NucliaDatasetsExport(
            client=client,
            datasets_url=nucliadb_args.datasets_url,
            trainset=trainset,
            cache_path=nucliadb_args.download_path,
            apikey=nucliadb_args.apikey,
        )
        fse.export()
    elif nucliadb_args.export == ExportType.FILESYSTEM:
        client = NucliaDBClient(
            environment=nucliadb_args.environment,
            url=nucliadb_args.url,
            api_key=nucliadb_args.service_token,
        )
        fse = FileSystemExport(
            client=client,
            trainset=trainset,
            store_path=nucliadb_args.download_path,
        )
        fse.export()
