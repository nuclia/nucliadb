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
from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_sdk.client import Environment, NucliaDBClient
from nucliadb_protos.train_pb2 import TrainSet


class NucliaDatasetsExport:
    def __init__(
        self,
        apikey: str,
        nucliadb_kb_url: str,
        trainset: TrainSet,
        cache_path: str,
        environment: Environment,
        service_token: Optional[str] = None,
    ):
        self.apikey = apikey
        self.client = NucliaDBClient(
            environment=environment, url=nucliadb_kb_url, api_key=service_token
        )
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, client=self.client, base_path=cache_path
        )

    def export(self):
        # Show progress
        for partition, filename in self.nucliadb_dataset.iter_all_partitions():
            # Upload to NucliaDatasetService
            pass


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
            environment=environment, url=nucliadb_kb_url, service_token=service_token
        )
        self.nucliadb_dataset = NucliaDBDataset(
            trainset=trainset, client=self.client, base_path=store_path
        )

    def export(self):
        self.nucliadb_dataset.generate_all_partitions()
