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
import tempfile

import pyarrow as pa  # type: ignore
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.export import FileSystemExport
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_filesystem(knowledgebox: KnowledgeBox, upload_data_field_classification):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
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
                assert len(loaded_array) == 2
