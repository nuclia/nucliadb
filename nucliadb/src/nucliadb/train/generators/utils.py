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

from typing import Any, AsyncGenerator, AsyncIterator, Optional, Type

from nucliadb.common.cache import get_resource_cache
from nucliadb.common.ids import FIELD_TYPE_STR_TO_PB
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.train import SERVICE_NAME, logger
from nucliadb.train.types import T
from nucliadb_utils.utilities import get_storage


async def get_resource_from_cache_or_db(kbid: str, uuid: str) -> Optional[ResourceORM]:
    resource_cache = get_resource_cache()
    if resource_cache is None:
        return await _get_resource_from_db(kbid, uuid)
        logger.warning("Resource cache is not set")

    return await resource_cache.get(kbid, uuid)


async def _get_resource_from_db(kbid: str, uuid: str) -> Optional[ResourceORM]:
    storage = await get_storage(service_name=SERVICE_NAME)
    async with get_driver().transaction(read_only=True) as transaction:
        kb = KnowledgeBoxORM(transaction, storage, kbid)
        return await kb.get(uuid)


async def get_paragraph(kbid: str, paragraph_id: str) -> str:
    if paragraph_id.count("/") == 5:
        rid, field_type, field, split_str, start_end = paragraph_id.split("/")
        split = int(split_str)
        start_str, end_str = start_end.split("-")
    else:
        rid, field_type, field, start_end = paragraph_id.split("/")
        split = None
        start_str, end_str = start_end.split("-")
    start = int(start_str)
    end = int(end_str)

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

    if split is not None:
        text = extracted_text.split_text[split]
        splitted_text = text[start:end]
    else:
        splitted_text = extracted_text.text[start:end]

    return splitted_text


async def batchify(
    producer: AsyncIterator[Any], size: int, batch_klass: Type[T]
) -> AsyncGenerator[T, None]:
    # NOTE: we are supposing all protobuffers have a data field
    batch = []
    async for item in producer:
        batch.append(item)
        if len(batch) == size:
            batch_pb = batch_klass(data=batch)
            yield batch_pb
            batch = []

    if len(batch):
        batch_pb = batch_klass(data=batch)
        yield batch_pb
