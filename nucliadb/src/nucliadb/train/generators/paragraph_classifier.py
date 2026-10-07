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

from collections.abc import AsyncGenerator

from fastapi import HTTPException
from nidx_protos.nodereader_pb2 import StreamRequest

from nucliadb.train.generators.utils import batchify
from nucliadb_models.filters import FilterExpression
from nucliadb_protos.dataset_pb2 import (
    ParagraphClassificationBatch,
    TextLabel,
    TrainSet,
)


def paragraph_classification_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
    filter_expression: FilterExpression | None,
) -> AsyncGenerator[ParagraphClassificationBatch, None]:
    if len(trainset.filter.labels) != 1:
        raise HTTPException(
            status_code=422,
            detail="Paragraph Classification should be of 1 labelset",
        )

    generator = generate_paragraph_classification_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, ParagraphClassificationBatch)
    return batch_generator


async def generate_paragraph_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[TextLabel, None]:
    labelset = f"/l/{trainset.filter.labels[0]}"

    # Query how many paragraphs has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    request.filter.labels.append(labelset)

    # TODO(Marklogic): Implement proper handling of paragraph classification
    if False:
        yield
