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

from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb_dataset.tests.integration.utils import export_dataset
from nucliadb_sdk.knowledgebox import KnowledgeBox


def test_paragraph_streaming(text_editors_kb: KnowledgeBox):
    knowledgebox = text_editors_kb

    trainset = TrainSet()
    trainset.type = TaskType.PARAGRAPH_STREAMING
    trainset.batch_size = 10

    partitions = export_dataset(knowledgebox, trainset)
    assert len(partitions) == 1

    loaded_array = partitions[0]
    assert len(loaded_array) == 8  # 4 resources with title and summary
