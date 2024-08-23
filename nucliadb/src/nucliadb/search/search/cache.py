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

import asyncio
from contextvars import ContextVar
from typing import Optional

from lru import LRU

from nucliadb.common.ids import FieldId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb.search import SERVICE_NAME
from nucliadb_telemetry import metrics
from nucliadb_utils.utilities import get_storage
import logging

logger = logging.getLogger(__name__)

rcache: ContextVar[Optional[dict[str, ResourceORM]]] = ContextVar("rcache", default=None)
etcache: ContextVar[Optional["ExtractedTextCache"]] = ContextVar("etcache", default=None)


RESOURCE_LOCKS: dict[str, asyncio.Lock] = LRU(1000)  # type: ignore
RESOURCE_CACHE_OPS = metrics.Counter("nucliadb_resource_cache_ops", labels={"type": ""})
EXTRACTED_CACHE_OPS = metrics.Counter("nucliadb_extracted_text_cache_ops", labels={"type": ""})


def set_extracted_text_cache(cache: "ExtractedTextCache") -> None:
    etcache.set(cache)


def get_extracted_text_cache() -> Optional["ExtractedTextCache"]:
    return etcache.get()


def clear_extracted_text_cache() -> None:
    cache = etcache.get()
    if cache is not None:
        cache.clear()

def get_resource_cache(clear: bool = False) -> dict[str, ResourceORM]:
    value: Optional[dict[str, ResourceORM]] = rcache.get()
    if value is None or clear:
        value = {}
        rcache.set(value)
    return value


async def get_resource_from_cache(kbid: str, uuid: str) -> Optional[ResourceORM]:
    orm_resource: Optional[ResourceORM] = None

    resource_cache = get_resource_cache()

    if uuid not in RESOURCE_LOCKS:
        RESOURCE_LOCKS[uuid] = asyncio.Lock()

    async with RESOURCE_LOCKS[uuid]:
        if uuid not in resource_cache:
            RESOURCE_CACHE_OPS.inc({"type": "miss"})
            async with get_driver().transaction(read_only=True) as txn:
                storage = await get_storage(service_name=SERVICE_NAME)
                kb = KnowledgeBoxORM(txn, storage, kbid)
                orm_resource = await kb.get(uuid)
        else:
            RESOURCE_CACHE_OPS.inc({"type": "hit"})

        if orm_resource is not None:
            resource_cache[uuid] = orm_resource
        else:
            orm_resource = resource_cache.get(uuid)

    return orm_resource


class ExtractedTextCache:
    """
    Used to cache extracted text from a resource in memory during the process
    of search results hydration.

    This is needed to avoid fetching the same extracted text multiple times,
    as matching text blocks are processed in parallel and the extracted text is
    fetched for each field where the text block is found.
    """

    def __init__(self):
        self.locks = {}
        self.values = {}

    def get_value(self, key: str) -> Optional[ExtractedText]:
        return self.values.get(key)

    def get_lock(self, key: str) -> asyncio.Lock:
        return self.locks.setdefault(key, asyncio.Lock())

    def set_value(self, key: str, value: ExtractedText) -> None:
        self.values[key] = value

    def clear(self):
        self.values.clear()
        self.locks.clear()


async def get_field_extracted_text(field: Field) -> Optional[ExtractedText]:
    cache = get_extracted_text_cache()
    if cache is None:
        logger.warning("Extracted text cache not set")
        EXTRACTED_CACHE_OPS.inc({"type": "miss"})
        return await field.get_extracted_text()

    key = f"{field.kbid}/{field.uuid}/{field.id}"
    extracted_text = cache.get_value(key)
    if extracted_text is not None:
        EXTRACTED_CACHE_OPS.inc({"type": "hit"})
        return extracted_text

    async with cache.get_lock(key):
        # Check again in case another task already fetched it
        extracted_text = cache.get_value(key)
        if extracted_text is not None:
            EXTRACTED_CACHE_OPS.inc({"type": "hit"})
            return extracted_text

        EXTRACTED_CACHE_OPS.inc({"type": "miss"})
        extracted_text = await field.get_extracted_text()
        if extracted_text is not None:
            # Only cache if we actually have extracted text
            cache.set_value(key, extracted_text)
        return extracted_text


async def get_field_extracted_text_from_cache(
    kbid: str, rid: str, field_id: str
) -> Optional[ExtractedText]:
    orm_resource = await get_resource_from_cache(kbid, rid)
    if orm_resource is None:
        return None
    field = await orm_resource.get_field(
        key=field
        type=field.pb_type(),
        load=False,
    )
    return await get_field_extracted_text(field)