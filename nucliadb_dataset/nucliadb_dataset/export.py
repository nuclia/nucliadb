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

from typing import Optional

import requests
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_sdk.client import Environment, NucliaDBClient


class NucliaDatasetsExport:
    def __init__(
        self,
        apikey: str,
        nucliadb_kb_url: str,
        datasets_url: str,
        trainset: TrainSet,
        cache_path: str,
        environment: Environment,
        service_token: Optional[str] = None,
    ):
        self.apikey = apikey
        self.datasets_url = datasets_url
        self.trainset = trainset
        self.client = NucliaDBClient(
            environment=environment, url=nucliadb_kb_url, api_key=service_token
        )
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, client=self.client, base_path=cache_path
        )
        self.datasets_url

    def export(self):
        dataset_def = {
            "type": TaskType.Name(self.trainset.type),
            "filter": {"labels": list(self.trainset.filter.labels)},
        }
        response = requests.post(
            f"{self.datasets_url}/datasets",
            json=dataset_def,
            headers={"x_stf_nuakey": self.apikey},
        )

        dataset_id = response.json()["id"]

        # Show progress
        for partition_id, filename in self.nucliadb_dataset.iter_all_partitions():
            # Upload to NucliaDatasetService
            with open(filename, "rb") as partition_fileobj:
                requests.put(
                    f"{self.datasets_url}/dataset/{dataset_id}/partition/{partition_id}",
                    data=partition_fileobj,
                    headers={"x_stf_nuakey": self.apikey},
                )


class FileSystemExport:
    def __init__(
        self,
        nucliadb_kb_url: str,
        trainset: TrainSet,
        store_path: str,
        environment: Environment,
        service_token: Optional[str] = None,
    ):
        self.client = NucliaDBClient(
            environment=environment, url=nucliadb_kb_url, api_key=service_token
        )
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, client=self.client, base_path=store_path
        )

    def export(self):
        self.nucliadb_dataset.read_all_partitions()
