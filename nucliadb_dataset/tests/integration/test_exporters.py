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

import os
import re
import tempfile
from uuid import uuid4

import pyarrow as pa  # type: ignore
import pytest
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.v2.sdk import NucliaDB


def test_filesystem_export(
    sdk: NucliaDB, upload_data_field_classification: KnowledgeBoxObj
):
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
