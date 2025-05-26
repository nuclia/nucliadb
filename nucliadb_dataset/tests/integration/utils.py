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

import tempfile

import pyarrow as pa  # type: ignore

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_protos.dataset_pb2 import TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB


def export_dataset(sdk: NucliaDB, trainset: TrainSet, kb: KnowledgeBoxObj) -> list[pa.Table]:
    with tempfile.TemporaryDirectory() as tmpdirname:
        dataset = NucliaDBDataset(
            sdk=sdk,
            kbid=kb.uuid,
            trainset=trainset,
            base_path=tmpdirname,
        )
        arrays = []
        for filename in dataset.read_all_partitions():
            with pa.memory_map(filename, "rb") as source:
                loaded_array = pa.ipc.open_stream(source).read_all()
                # We multiply by two due to auto-generated title field
                arrays.append(loaded_array)
        return arrays
