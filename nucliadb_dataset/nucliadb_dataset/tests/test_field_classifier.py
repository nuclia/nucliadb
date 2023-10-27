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
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.dataset import (
    NucliaDataset,
    NucliaDBDataset,
    download_all_partitions,
)
from nucliadb_dataset.export import FileSystemExport, NucliaDatasetsExport
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_filesystem(knowledgebox: KnowledgeBox, upload_data_field_classification):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = FileSystemExport(
            knowledgebox.client,
            trainset=trainset,
            store_path=tmpdirname,
        )
        fse.export()
        files = os.listdir(tmpdirname)
        for filename in files:
            with pa.memory_map(f"{tmpdirname}/{filename}", "rb") as source:
                loaded_array = pa.ipc.open_stream(source).read_all()
                # We multiply by two due to auto-generated title field
                assert len(loaded_array) == 2 * 2


def run_dataset_export(requests_mock, knowledgebox, trainset):
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

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = NucliaDatasetsExport(
            client=knowledgebox.client,
            datasets_url="http://datasets.service",
            trainset=trainset,
            cache_path=tmpdirname,
            apikey="i_am_a_nuakey",
        )
        fse.export()


def test_nucliadb_export_fields(
    knowledgebox: KnowledgeBox, upload_data_field_classification, requests_mock
):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    run_dataset_export(requests_mock, knowledgebox, trainset)


def test_inst_base_class():
    try:
        NucliaDataset()
    except TypeError:
        pass
    else:
        raise AssertionError("Base class should'nt be instantiable")


def test_live_field_classification(
    knowledgebox: KnowledgeBox, upload_data_field_classification
):
    trainset = TrainSet()
    trainset.type = TaskType.FIELD_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with tempfile.TemporaryDirectory() as tmpdirname:
        fse = NucliaDBDataset(
            client=knowledgebox.client,
            trainset=trainset,
            base_path=tmpdirname,
        )
        partitions = fse.get_partitions()
        assert len(partitions) == 1
        filename = fse.read_partition(partitions[0])

        with pa.memory_map(filename, "rb") as source:
            loaded_array = pa.ipc.open_stream(source).read_all()
            # We multiply by two due to auto-generated title field
            assert len(loaded_array) == 2 * 2


def test_datascientist(knowledgebox: KnowledgeBox, temp_folder):
    knowledgebox.upload(
        text="I'm Ramon",
        labels=["labelset/positive"],
    )

    knowledgebox.upload(
        text="I'm not Ramon",
        labels=["labelset/negative"],
    )

    knowledgebox.upload(
        text="I'm Aleix",
        labels=["labelset/smart"],
    )

    arrow_filenames = download_all_partitions(
        task="FIELD_CLASSIFICATION",
        knowledgebox=knowledgebox,
        path=temp_folder,
        labels=["labelset"],
    )

    for filename in arrow_filenames:
        with pa.memory_map(filename, "rb") as source:
            loaded_array = pa.ipc.open_stream(source).read_all()
            # We multiply by two due to auto-generated title field
            assert len(loaded_array) == 3 * 2
