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
from nucliadb_protos.dataset_pb2 import (
    FieldSplitData,
    FieldStreamingBatch,
    TrainSet,
)
from nucliadb_protos.resources_pb2 import Basic, FieldComputedMetadata
from nucliadb_protos.utils_pb2 import ExtractedText


def field_streaming_batch_generator(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[FieldStreamingBatch, None]:
    generator = generate_field_streaming_payloads(kbid, trainset, shard_replica_id)
    batch_generator = batchify(generator, trainset.batch_size, FieldStreamingBatch)
    return batch_generator


async def generate_field_streaming_payloads(
    kbid: str,
    trainset: TrainSet,
    shard_replica_id: str,
) -> AsyncGenerator[FieldSplitData, None]:
    # Query how many resources has each label
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    for label in trainset.filter.labels:
        request.filter.labels.append(f"/l/{label}")

    for path in trainset.filter.paths:
        request.filter.labels.append(f"/p/{path}")

    for metadata in trainset.filter.metadata:
        request.filter.labels.append(f"/m/{metadata}")

    for entity in trainset.filter.entities:
        request.filter.labels.append(f"/e/{entity}")

    for field in trainset.filter.fields:
        request.filter.labels.append(f"/f/{field}")

    for status in trainset.filter.status:
        request.filter.labels.append(f"/n/s/{status}")

    total = 0
    resources = set()

    async for document_item in get_nidx_searcher_client().Documents(request):
        text_labels = []
        for label in document_item.labels:
            text_labels.append(label)

        field_id = f"{document_item.uuid}{document_item.field}"
        total += 1
        resources.add(document_item.uuid)

        field_parts = document_item.field.split("/")
        if len(field_parts) == 3:
            _, field_type, field = field_parts
            split = "0"
        elif len(field_parts) == 4:
            _, field_type, field, split = field_parts
        else:
            raise Exception(f"Invalid field definition {document_item.field}")

        tl = FieldSplitData()
        rid, field_type, field = field_id.split("/")
        tl.rid = document_item.uuid
        tl.field = field
        tl.field_type = field_type
        tl.split = split

        if trainset.exclude_text:
            tl.text.text = ""
        else:
            extracted = await get_field_text(kbid, rid, field, field_type)
            if extracted is not None:
                tl.text.CopyFrom(extracted)

        metadata_obj = await get_field_metadata(kbid, rid, field, field_type)
        if metadata_obj is not None:
            tl.metadata.CopyFrom(metadata_obj)

        basic = await get_field_basic(kbid, rid, field, field_type)
        if basic is not None:
            tl.basic.CopyFrom(basic)

        tl.labels.extend(text_labels)

        yield tl

        if total % 1000 == 0:
            logger.info(
                "Field streaming in progress",
                extra={
                    "fields": total,
                    "resources": len(resources),
                    "kbid": kbid,
                    "shard_replica_id": shard_replica_id,
                },
            )

    logger.info(
        "Field streaming finished",
        extra={
            "fields": total,
            "resources": len(resources),
            "kbid": kbid,
            "shard_replica_id": shard_replica_id,
        },
    )


async def get_field_text(kbid: str, rid: str, field: str, field_type: str) -> Optional[ExtractedText]:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return None

    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    extracted_text = await field_obj.get_extracted_text()

    return extracted_text


async def get_field_metadata(
    kbid: str, rid: str, field: str, field_type: str
) -> Optional[FieldComputedMetadata]:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return None

    field_type_int = FIELD_TYPE_STR_TO_PB[field_type]
    field_obj = await orm_resource.get_field(field, field_type_int, load=False)
    field_metadata = await field_obj.get_field_metadata()

    return field_metadata


async def get_field_basic(kbid: str, rid: str, field: str, field_type: str) -> Optional[Basic]:
    orm_resource = await get_resource_from_cache_or_db(kbid, rid)

    if orm_resource is None:
        logger.error(f"{rid} does not exist on DB")
        return None

    basic = await orm_resource.get_basic()

    return basic
