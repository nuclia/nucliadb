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

import tempfile

import pyarrow as pa  # type: ignore
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.dataset import NucliaDBDataset, download_all_partitions
from nucliadb_sdk.knowledgebox import KnowledgeBox


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
