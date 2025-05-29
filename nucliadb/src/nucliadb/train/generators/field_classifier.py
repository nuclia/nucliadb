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

from typing import AsyncGenerator, Optional

from nidx_protos.nodereader_pb2 import StreamRequest

from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.train import logger
from nucliadb.train.generators.utils import batchify, get_resource_from_cache_or_db
from nucliadb_models.filters import FilterExpression
from nucliadb_protos.dataset_pb2 import (
    FieldClassificationBatch,
    Label,
    TextLabel,
    TrainSet,
)


def field_classification_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
    filter_expression: Optional[FilterExpression],
) -> AsyncGenerator[FieldClassificationBatch, None]:
    generator = generate_field_classification_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, FieldClassificationBatch)
    return batch_generator


async def generate_field_classification_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[TextLabel, None]:
    labelset = f"/l/{trainset.filter.labels[0]}"

    # Query how many resources has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id
    request.filter.labels.append(labelset)
    total = 0

    async for document_item in get_nidx_searcher_client().Documents(request):
        text_labels = []
        for label in document_item.labels:
            if label.startswith(labelset):
                text_labels.append(label)

        field_id = f"{document_item.uuid}{document_item.field}"
        total += 1

        tl = TextLabel()
        rid, field_type, field = field_id.split("/")
        tl.text = await get_field_text(kbid, rid, field, field_type)

        for label in text_labels:
            _, _, labelset_title, label_title = label.split("/")
            tl.labels.append(Label(labelset=labelset_title, label=label_title))

        yield tl


async def get_field_text(kbid: str, rid: str, field: str, field_type: str) -> str:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return ""

    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()
    if extracted_text is None:
        logger.warning(f"{rid} {field} {field_type_int} extracted_text does not exist on DB")
        return ""

    text = ""
    for _, split in extracted_text.split_text.items():
        text += split
        text += " "
    text += extracted_text.text

    return text
