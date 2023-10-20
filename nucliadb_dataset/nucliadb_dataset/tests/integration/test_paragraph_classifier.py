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

import pytest
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.tests.integration.utils import export_dataset
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_paragraph_classification_with_labels(
    knowledgebox: KnowledgeBox, upload_data_paragraph_classification
):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.batch_size = 2

    tests = [
        ([], 3),
        (["labelset1"], 3),
        (["labelset2"], 1),
    ]
    for labels, expected in tests:
        trainset.filter.ClearField("labels")
        trainset.filter.labels.extend(labels)  # type: ignore

        partitions = export_dataset(knowledgebox, trainset)
        assert len(partitions) == 1

        loaded_array = partitions[0]
        assert len(loaded_array) == expected


def test_paragraph_classification_without_labels(
    knowledgebox: KnowledgeBox, upload_data_paragraph_classification_unlabeled
):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.batch_size = 2

    tests = [
        ([], 3),
        (["labelset1"], 0),
        (["labelset2"], 0),
    ]
    for labels, expected in tests:
        trainset.filter.ClearField("labels")
        trainset.filter.labels.extend(labels)  # type: ignore

        partitions = export_dataset(knowledgebox, trainset)
        assert len(partitions) == 1

        loaded_array = partitions[0]
        assert len(loaded_array) == expected


def test_paragraph_classification_invalid_label_type(
    knowledgebox: KnowledgeBox, upload_data_field_classification
):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.filter.labels.append("labelset1")
    trainset.batch_size = 2

    with pytest.raises(Exception):
        export_dataset(knowledgebox, trainset)


def test_paragraph_classification_populates_field_ids(
    knowledgebox: KnowledgeBox, upload_data_paragraph_classification
):
    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_CLASSIFICATION
    trainset.batch_size = 2

    partitions = export_dataset(knowledgebox, trainset)
    assert len(partitions) == 1

    loaded_array = partitions[0]
    assert len(loaded_array) == 3

    for paragraph_id in loaded_array["paragraph_id"]:
        # only title is indexed, for field content the processor is needed and
        # we are not mocking it
        assert "/a/title/" in str(paragraph_id)
