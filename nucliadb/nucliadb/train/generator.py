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


from fastapi import HTTPException
from nucliadb_protos.dataset_pb2 import TaskType, TrainSet

from nucliadb.train.generators.field_classifier import (
    generate_field_classification_payloads,
)
from nucliadb.train.generators.image_classifier import (
    generate_image_classification_payloads,
)
from nucliadb.train.generators.paragraph_classifier import (
    generate_paragraph_classification_payloads,
)
from nucliadb.train.generators.sentence_classifier import (
    generate_sentence_classification_payloads,
)
from nucliadb.train.generators.token_classifier import (
    generate_token_classification_payloads,
)
from nucliadb.train.generators.utils import get_transaction
from nucliadb.train.utils import get_shard_manager


async def generate_train_data(kbid: str, shard: str, trainset: TrainSet):
    # Get the data structure to generate data
    shard_manager = get_shard_manager()
    node, shard_replica_id = await shard_manager.get_reader(kbid, shard)

    if trainset.batch_size == 0:
        trainset.batch_size = 50

    if trainset.type == TaskType.PARAGRAPH_CLASSIFICATION:
        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Paragraph Classification should be of 1 labelset",
            )

        async for paragraph_data in generate_paragraph_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = paragraph_data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == TaskType.FIELD_CLASSIFICATION:
        if len(trainset.filter.labels) != 1:
            raise HTTPException(
                status_code=422,
                detail="Field Classification should be of 1 labelset",
            )

        async for field_data in generate_field_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = field_data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == TaskType.TOKEN_CLASSIFICATION:
        async for token_data in generate_token_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = token_data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == TaskType.SENTENCE_CLASSIFICATION:
        if len(trainset.filter.labels) == 0:
            raise HTTPException(
                status_code=422,
                detail="Sentence Classification should be at least of 1 labelset",
            )

        async for sentence_data in generate_sentence_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = sentence_data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    if trainset.type == TaskType.IMAGE_CLASSIFICATION:
        async for image_data in generate_image_classification_payloads(
            kbid, trainset, node, shard_replica_id
        ):
            payload = image_data.SerializeToString()
            yield len(payload).to_bytes(4, byteorder="big", signed=False)
            yield payload

    txn = await get_transaction()
    await txn.abort()
