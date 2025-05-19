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

import pyarrow as pa  # type: ignore

from integration.utils import export_dataset
from nucliadb_dataset.dataset import download_all_partitions
from nucliadb_models.common import UserClassification
from nucliadb_models.metadata import UserMetadata
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB


def test_field_classification_with_labels(
    sdk: NucliaDB, upload_data_field_classification: KnowledgeBoxObj
):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.batch_size = 2

    # We multiply by two due to auto-generated title field
    tests = [
        (["labelset1"], 2 * 2),
        (["labelset2"], 1 * 2),
    ]
    for labels, expected in tests:
        trainset.filter.ClearField("labels")
        trainset.filter.labels.extend(labels)

        partitions = export_dataset(sdk=sdk, trainset=trainset, kb=upload_data_field_classification)
        assert len(partitions) == 1

        loaded_array = partitions[0]
        assert len(loaded_array) == expected


def test_datascientist(sdk: NucliaDB, temp_folder, kb: KnowledgeBoxObj):
    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            texts={FieldIdString("text"): TextField(body="I'm Ramon")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset", label="positive"),
                ]
            ),
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            texts={FieldIdString("text"): TextField(body="I'm not Ramon")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset", label="negative"),
                ]
            ),
        ),
    )

    sdk.create_resource(
        kbid=kb.uuid,
        content=CreateResourcePayload(
            texts={FieldIdString("text"): TextField(body="I'm Aleix")},
            usermetadata=UserMetadata(
                classifications=[
                    UserClassification(labelset="labelset", label="smart"),
                ]
            ),
        ),
    )

    arrow_filenames = download_all_partitions(
        task="FIELD_CLASSIFICATION",
        sdk=sdk,
        kbid=kb.uuid,
        path=temp_folder,
        labels=["labelset"],
    )

    for filename in arrow_filenames:
        with pa.memory_map(filename, "rb") as source:
            loaded_array = pa.ipc.open_stream(source).read_all()
            # We multiply by two due to auto-generated title field
            assert len(loaded_array) == 3 * 2
