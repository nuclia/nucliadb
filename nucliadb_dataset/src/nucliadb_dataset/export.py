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


import requests

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
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
