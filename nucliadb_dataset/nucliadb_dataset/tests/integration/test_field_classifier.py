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

from nucliadb_models.common import UserClassification
from nucliadb_models.metadata import UserMetadata
from nucliadb_models.resource import KnowledgeBoxObj
from nucliadb_models.text import TextField
from nucliadb_models.utils import FieldIdString, SlugString
from nucliadb_models.writer import CreateResourcePayload
from nucliadb_sdk.v2.sdk import NucliaDB
import pyarrow as pa  # type: ignore
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.dataset import download_all_partitions
from nucliadb_dataset.tests.integration.utils import export_dataset
from nucliadb_sdk.knowledgebox import KnowledgeBox


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
        trainset.filter.labels.extend(labels)  # type: ignore

        partitions = export_dataset(
            sdk=sdk, trainset=trainset, kb=upload_data_field_classification
        )
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
        slug=kb.slug,
        path=temp_folder,
        labels=["labelset"],
    )

    for filename in arrow_filenames:
        with pa.memory_map(filename, "rb") as source:
            loaded_array = pa.ipc.open_stream(source).read_all()
            # We multiply by two due to auto-generated title field
            assert len(loaded_array) == 3 * 2
