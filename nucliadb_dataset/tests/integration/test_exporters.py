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

import os
import re
import tempfile
from uuid import uuid4

import pyarrow as pa  # type: ignore
import pytest

from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet
from nucliadb_sdk.v2.sdk import NucliaDB


def test_filesystem_export(sdk: NucliaDB, upload_data_field_classification: KnowledgeBoxObj):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        exporter = FileSystemExport(
            sdk=sdk,
            kbid=upload_data_field_classification.uuid,
            trainset=trainset,
            store_path=tmpdirname,
        )
        exporter.export()
        files = os.listdir(tmpdirname)
        for filename in files:
            with pa.memory_map(f"{tmpdirname}/{filename}", "rb") as source:
                loaded_array = pa.ipc.open_stream(source).read_all()
                # We multiply by two due to auto-generated title field
                assert len(loaded_array) == 2 * 2


def test_datasets_export(
    mocked_datasets_url: str,
    sdk: NucliaDB,
    upload_data_field_classification: KnowledgeBoxObj,
):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        exporter = NucliaDatasetsExport(
            sdk=sdk,
            kbid=upload_data_field_classification.uuid,
            datasets_url=mocked_datasets_url,
            trainset=trainset,
            cache_path=tmpdirname,
            apikey="i_am_a_nuakey",
        )
        exporter.export()

    # TODO: we could improve the mock return value


@pytest.fixture(scope="function")
def mocked_datasets_url(requests_mock):
    mock_dataset_id = str(uuid4())
    requests_mock.real_http = True
    requests_mock.register_uri(
        "POST",
        "http://datasets.service/datasets",
        status_code=201,
        json={"id": mock_dataset_id},
    )
    requests_mock.register_uri(
        "PUT",
        re.compile(rf"^http://datasets.service/dataset/{mock_dataset_id}/partition/.*"),
        status_code=204,
        content=b"",
    )
    yield "http://datasets.service"
