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
#

from typing import AsyncIterator, Optional

from fastapi import HTTPException
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb.train.generators.field_classifier import (
    field_classification_batch_generator,
)
from nucliadb.train.generators.image_classifier import (
    image_classification_batch_generator,
)
from nucliadb.train.generators.paragraph_classifier import (
    paragraph_classification_batch_generator,
)
from nucliadb.train.generators.paragraph_streaming import (
    paragraph_streaming_batch_generator,
)
from nucliadb.train.generators.sentence_classifier import (
    sentence_classification_batch_generator,
)
from nucliadb.train.generators.token_classifier import (
    token_classification_batch_generator,
)
from nucliadb.train.generators.utils import get_transaction
from nucliadb.train.types import TrainBatch
from nucliadb.train.utils import get_shard_manager


async def generate_train_data(kbid: str, shard: str, trainset: TrainSet):
    # Get the data structure to generate data
    shard_manager = get_shard_manager()
    node, shard_replica_id = await shard_manager.get_reader(kbid, shard)

    if trainset.batch_size == 0:
        trainset.batch_size = 50

    batch_generator: Optional[AsyncIterator[TrainBatch]] = None

    if trainset.type == TaskType.FIELD_CLASSIFICATION:
        batch_generator = field_classification_batch_generator(
            kbid, trainset, node, shard_replica_id
        )
    elif trainset.type == TaskType.IMAGE_CLASSIFICATION:
        batch_generator = image_classification_batch_generator(
            kbid, trainset, node, shard_replica_id
        )
    elif trainset.type == TaskType.PARAGRAPH_CLASSIFICATION:
        batch_generator = paragraph_classification_batch_generator(
            kbid, trainset, node, shard_replica_id
        )
    elif trainset.type == TaskType.TOKEN_CLASSIFICATION:
        batch_generator = token_classification_batch_generator(
            kbid, trainset, node, shard_replica_id
        )
    elif trainset.type == TaskType.SENTENCE_CLASSIFICATION:
        batch_generator = sentence_classification_batch_generator(
            kbid, trainset, node, shard_replica_id
        )
    elif trainset.type == TaskType.PARAGRAPH_STREAMING:
        batch_generator = paragraph_streaming_batch_generator(
            kbid, trainset, node, shard_replica_id
        )

    if batch_generator is None:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid train type '{TaskType.Name(trainset.type)}'",
        )

    async for item in batch_generator:
        payload = item.SerializeToString()
        yield len(payload).to_bytes(4, byteorder="big", signed=False)
        yield payload

    txn = await get_transaction()
    await txn.abort()
