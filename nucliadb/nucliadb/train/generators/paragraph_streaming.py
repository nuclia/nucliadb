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

from typing import AsyncIterator

from nucliadb_protos.dataset_pb2 import Paragraph, ParagraphStreamingBatch, TrainSet
from nucliadb_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.train.generators.utils import get_paragraph


async def generate_paragraph_streaming_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncIterator[ParagraphStreamingBatch]:
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    batch = ParagraphStreamingBatch()
    async for paragraph_item in node.stream_get_paragraphs(request):
        paragraph_id = paragraph_item.id
        text = await get_paragraph(kbid, paragraph_id)

        paragraph = Paragraph(
            id=paragraph_id,
            text=text,
        )
        batch.data.append(paragraph)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphStreamingBatch()
    if len(batch.data):
        yield batch
