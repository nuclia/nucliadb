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

import asyncio
from typing import AsyncGenerator, AsyncIterable, Optional

from nidx_protos.nodereader_pb2 import DocumentItem, StreamRequest

from nucliadb.common.filter_expression import parse_expression
from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.train import logger
from nucliadb.train.generators.utils import batchify, get_resource_from_cache_or_db
from nucliadb.train.settings import settings
from nucliadb_models.filters import (
    FilterExpression,
)
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
    filter_expression: Optional[FilterExpression],
) -> AsyncGenerator[FieldStreamingBatch, None]:
    generator = generate_field_streaming_payloads(kbid, trainset, shard_replica_id, filter_expression)
    batch_generator = batchify(generator, trainset.batch_size, FieldStreamingBatch)
    return batch_generator


async def generate_field_streaming_payloads(
    kbid: str, trainset: TrainSet, shard_replica_id: str, filter_expression: Optional[FilterExpression]
) -> AsyncGenerator[FieldSplitData, None]:
    request = StreamRequest()
    request.shard_id.id = shard_replica_id

    if filter_expression:
        await parse_filter_expression(kbid, request, filter_expression)
    else:
        parse_legacy_filters(request, trainset)

    resources = set()
    fields = set()

    async for fsd in iter_field_split_data(
        request, kbid, trainset, max_parallel=settings.field_streaming_parallelisation
    ):
        resources.add(fsd.rid)
        field_unique_key = f"{fsd.rid}/{fsd.field_type}/{fsd.field}/{fsd.split}"
        if field_unique_key in fields:
            # This field has already been yielded. This can happen as we are streaming directly from nidx
            # and field deletions may not be reflected immediately in the index.
            logger.warning(f"Duplicated field found {field_unique_key}. Skipping.", extra={"kbid": kbid})
            continue
        fields.add(field_unique_key)

        yield fsd

        if len(fields) % 1000 == 0:
            logger.info(
                "Field streaming in progress",
                extra={
                    "fields": len(fields),
                    "resources": len(resources),
                    "kbid": kbid,
                    "shard_replica_id": shard_replica_id,
                },
            )

    logger.info(
        "Field streaming finished",
        extra={
            "fields": len(fields),
            "resources": len(resources),
            "kbid": kbid,
            "shard_replica_id": shard_replica_id,
        },
    )


async def parse_filter_expression(
    kbid: str, request: StreamRequest, filter_expression: FilterExpression
):
    if filter_expression.field:
        expr = await parse_expression(filter_expression.field, kbid)
        if expr:
            request.filter_expression.CopyFrom(expr)


def parse_legacy_filters(request: StreamRequest, trainset: TrainSet):
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


async def iter_field_split_data(
    request: StreamRequest, kbid: str, trainset: TrainSet, max_parallel: int = 5
) -> AsyncIterable[FieldSplitData]:
    tasks: list[asyncio.Task] = []
    async for document_item in get_nidx_searcher_client().Documents(request):
        if len(tasks) >= max_parallel:
            results = await asyncio.gather(*tasks)
            for fsd in results:
                yield fsd
            tasks.clear()
        tasks.append(asyncio.create_task(fetch_field_split_data(document_item, kbid, trainset)))
    if len(tasks):
        results = await asyncio.gather(*tasks)
        for fsd in results:
            yield fsd
        tasks.clear()


async def fetch_field_split_data(
    document_item: DocumentItem, kbid: str, trainset: TrainSet
) -> FieldSplitData:
    field_id = f"{document_item.uuid}{document_item.field}"
    field_parts = document_item.field.split("/")
    if len(field_parts) == 3:
        _, field_type, field = field_parts
        split = "0"
    elif len(field_parts) == 4:
        _, field_type, field, split = field_parts
    else:
        raise Exception(f"Invalid field definition {document_item.field}")
    _, field_type, field = field_id.split("/")
    fsd = FieldSplitData()
    fsd.rid = document_item.uuid
    fsd.field = field
    fsd.field_type = field_type
    fsd.split = split
    tasks = []
    if trainset.exclude_text:
        fsd.text.text = ""
    else:
        tasks.append(asyncio.create_task(_fetch_field_extracted_text(kbid, fsd)))
    tasks.append(asyncio.create_task(_fetch_field_metadata(kbid, fsd)))
    tasks.append(asyncio.create_task(_fetch_basic(kbid, fsd)))
    await asyncio.gather(*tasks)
    fsd.labels.extend(document_item.labels)
    return fsd


async def _fetch_field_extracted_text(kbid: str, fsd: FieldSplitData):
    extracted = await get_field_text(kbid, fsd.rid, fsd.field, fsd.field_type)
    if extracted is not None:
        fsd.text.CopyFrom(extracted)


async def _fetch_field_metadata(kbid: str, fsd: FieldSplitData):
    metadata_obj = await get_field_metadata(kbid, fsd.rid, fsd.field, fsd.field_type)
    if metadata_obj is not None:
        fsd.metadata.CopyFrom(metadata_obj)


async def _fetch_basic(kbid: str, fsd: FieldSplitData):
    basic = await get_field_basic(kbid, fsd.rid, fsd.field, fsd.field_type)
    if basic is not None:
        fsd.basic.CopyFrom(basic)


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
