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

from integration.utils import export_dataset
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB


def test_paragraph_streaming(sdk: NucliaDB, text_editors_kb: KnowledgeBoxObj):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_STREAMING
    trainset.batch_size = 10

    partitions = export_dataset(sdk, trainset, text_editors_kb)
    assert len(partitions) == 1

    loaded_array = partitions[0]
    assert len(loaded_array) == 8  # 4 resources with title and summary
