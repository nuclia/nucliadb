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


import requests
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_sdk.v2.sdk import NucliaDB


class NucliaDatasetsExport:
    def __init__(
        self,
        sdk: NucliaDB,
        kbid: str,
        datasets_url: str,
        trainset: TrainSet,
        cache_path: str,
        apikey: str,
    ):
        self.datasets_url = datasets_url
        self.trainset = trainset
        self.sdk = sdk
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, kbid=kbid, sdk=sdk, base_path=cache_path
        )
        self.apikey = apikey

    def export(self):
        dataset_def = {
            "type": TaskType.Name(self.trainset.type),
            "filter": {"labels": list(self.trainset.filter.labels)},
            "name": str(self.sdk.base_url),
        }
        response = requests.post(
            f"{self.datasets_url}/datasets",
            json=dataset_def,
            headers={"x-stf-nuakey": f"Bearer {self.apikey}"},
        )

        dataset_id = response.json()["id"]

        # Show progress
        for partition_id, filename in self.nucliadb_dataset.iter_all_partitions():
            # Upload to NucliaDatasetService
            with open(filename, "rb") as partition_fileobj:
                requests.put(
                    f"{self.datasets_url}/dataset/{dataset_id}/partition/{partition_id}",
                    data=partition_fileobj,
                    headers={"x-stf-nuakey": f"Bearer {self.apikey}"},
                )


class FileSystemExport:
    def __init__(
        self,
        sdk: NucliaDB,
        kbid: str,
        trainset: TrainSet,
        store_path: str,
    ):
        self.sdk = sdk
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, kbid=kbid, sdk=sdk, base_path=store_path
        )

    def export(self):
        self.nucliadb_dataset.read_all_partitions()
