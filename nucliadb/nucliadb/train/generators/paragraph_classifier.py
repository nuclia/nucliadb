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

from nucliadb_protos.dataset_pb2 import (
    Label,
    ParagraphClassificationBatch,
    TextLabel,
    TrainSet,
)
from nucliadb_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.ingest.orm.resource import KB_REVERSE
from nucliadb.train import logger
from nucliadb.train.generators.utils import get_resource_from_cache_or_db


async def get_paragraph(kbid: str, result: str) -> str:
    if result.count("/") == 5:
        rid, field_type, field, split_str, start_end = result.split("/")
        split = int(split_str)
        start_str, end_str = start_end.split("-")
    else:
        rid, field_type, field, start_end = result.split("/")
        split = None
        start_str, end_str = start_end.split("-")
    start = int(start_str)
    end = int(end_str)

    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return ""

    field_type_int = KB_REVERSE[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warning(
            f"{rid} {field} {field_type_int} extracted_text does not exist on DB"
        )
        return ""

    if split is not None:
        text = extracted_text.split_text[split]
        splitted_text = text[start:end]
    else:
        splitted_text = extracted_text.text[start:end]

    return splitted_text


async def generate_paragraph_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    node: AbstractIndexNode,
    shard_replica_id: str,
) -> AsyncIterator[ParagraphClassificationBatch]:
    # Query how many paragraphs has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    labelset = None
    if len(trainset.filter.labels) > 0:
        labelset = f"/l/{trainset.filter.labels[0]}"
        request.filter.tags.append(labelset)

    batch = ParagraphClassificationBatch()

    async for paragraph_item in node.stream_get_paragraphs(request):
        text_labels = []

        for label in paragraph_item.labels:
            if labelset is None or label.startswith(labelset):
                text_labels.append(label)

        tl = TextLabel()
        tl.id = paragraph_item.id
        paragraph_text = await get_paragraph(kbid, paragraph_item.id)
        tl.text = paragraph_text
        for label in text_labels:
            _, _, label_labelset, label_title = label.split("/")
            tl.labels.append(Label(labelset=label_labelset, label=label_title))
        batch.data.append(tl)

        if len(batch.data) == trainset.batch_size:
            yield batch
            batch = ParagraphClassificationBatch()

    if len(batch.data):
        yield batch
