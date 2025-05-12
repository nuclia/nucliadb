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

from typing import AsyncGenerator

from nidx_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.train import logger
from nucliadb.train.generators.utils import batchify, get_resource_from_cache_or_db
from nucliadb_protos.dataset_pb2 import (
    ParagraphStreamingBatch,
    ParagraphStreamItem,
    TrainSet,
)


def paragraph_streaming_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[ParagraphStreamingBatch, None]:
    generator = generate_paragraph_streaming_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, ParagraphStreamingBatch)
    return batch_generator


async def generate_paragraph_streaming_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[ParagraphStreamItem, None]:
    """Streams paragraphs ordered as if they were read sequentially from each
    field.

    """
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    async for document_item in get_nidx_searcher_client().Documents(request):
        field_id = f"{document_item.uuid}{document_item.field}"
        rid, field_type, field = field_id.split("/")

        orm_resource = await get_resource_from_cache_or_db(kbid, rid)
        if orm_resource is None:
            logger.error(f"{rid} does not exist on DB")
            continue

        field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
        field_obj = await orm_resource.get_field(field, field_type_int, load=False)

        extracted_text = await field_obj.get_extracted_text()
        field_metadata = await field_obj.get_field_metadata()
        if field_metadata is None:
            logger.error(f"{field_id} does not have field metadata on DB")
            continue

        for paragraph in field_metadata.metadata.paragraphs:
            item = ParagraphStreamItem()
            item.id = f"{rid}/{field_type}/{field}/{paragraph.start}-{paragraph.end}"
            item.text = extracted_text.text[paragraph.start : paragraph.end]

            yield item

        for split, metadata in field_metadata.split_metadata.items():
            # REVIEW: do we have to care about ExtractedText.deleted_splits?
            split_text = extracted_text.split_text[split]

            for paragraph in metadata.paragraphs:
                item = ParagraphStreamItem()
                item.id = f"{rid}/{field_type}/{field}/{split}/{paragraph.start}-{paragraph.end}"
                item.text = split_text[paragraph.start : paragraph.end]

                yield item
