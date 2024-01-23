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
from nucliadb_protos.dataset_pb2 import TrainSet

from nucliadb_dataset.dataset import NucliaDBDataset
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_sdk.v2.sdk import NucliaDB


def export_dataset(
    sdk: NucliaDB, trainset: TrainSet, kb: KnowledgeBoxObj
) -> list[pa.Table]:
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
