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
import contextlib
import logging
from contextvars import ContextVar
from typing import Optional

from lru import LRU

from nucliadb.common.ids import FieldId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.search import SERVICE_NAME
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_telemetry import metrics
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)

rcache: ContextVar[Optional[dict[str, ResourceORM]]] = ContextVar("rcache", default=None)
etcache: ContextVar[Optional["ExtractedTextCache"]] = ContextVar("etcache", default=None)


RESOURCE_LOCKS: dict[str, asyncio.Lock] = LRU(1000)  # type: ignore
RESOURCE_CACHE_OPS = metrics.Counter("nucliadb_resource_cache_ops", labels={"type": ""})
EXTRACTED_CACHE_OPS = metrics.Counter("nucliadb_extracted_text_cache_ops", labels={"type": ""})


def set_extracted_text_cache() -> None:
    value = ExtractedTextCache()
    etcache.set(value)


def get_extracted_text_cache() -> Optional["ExtractedTextCache"]:
    return etcache.get()


def clear_extracted_text_cache() -> None:
    value = etcache.get()
    if value is not None:
        value.clear()
        etcache.set(None)


def set_resource_cache() -> None:
    value: dict[str, ResourceORM] = {}
    rcache.set(value)


def get_resource_cache() -> Optional[dict[str, ResourceORM]]:
    return rcache.get()


def clear_resource_cache() -> None:
    value = rcache.get()
    if value is not None:
        value.clear()
        rcache.set(None)


async def get_resource(kbid: str, uuid: str) -> Optional[ResourceORM]:
    """
    Will try to get the resource from the cache, if it's not there it will fetch it from the ORM and cache it.
    """
    orm_resource: Optional[ResourceORM] = None

    resource_cache = get_resource_cache()
    if resource_cache is None:
        RESOURCE_CACHE_OPS.inc({"type": "miss"})
        logger.warning("Resource cache not set")
        return await _orm_get_resource(kbid, uuid)

    if uuid not in RESOURCE_LOCKS:
        RESOURCE_LOCKS[uuid] = asyncio.Lock()

    async with RESOURCE_LOCKS[uuid]:
        if uuid not in resource_cache:
            RESOURCE_CACHE_OPS.inc({"type": "miss"})
            orm_resource = await _orm_get_resource(kbid, uuid)
        else:
            RESOURCE_CACHE_OPS.inc({"type": "hit"})

        if orm_resource is not None:
            resource_cache[uuid] = orm_resource
        else:
            orm_resource = resource_cache.get(uuid)

    return orm_resource


async def _orm_get_resource(kbid: str, uuid: str) -> Optional[ResourceORM]:
    async with get_driver().transaction(read_only=True) as txn:
        storage = await get_storage(service_name=SERVICE_NAME)
        kb = KnowledgeBoxORM(txn, storage, kbid)
        return await kb.get(uuid)


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


async def get_extracted_text_from_field_id(kbid: str, field: FieldId) -> Optional[ExtractedText]:
    rid = field.rid
    orm_resource = await get_resource(kbid, rid)
    if orm_resource is None:
        return None
    field_obj = await orm_resource.get_field(
        key=field.key,
        type=field.pb_type,
        load=False,
    )
    return await get_field_extracted_text(field_obj)


@contextlib.contextmanager
def request_caches():
    """
    This context manager sets the caches for extracted text and resources for a request.

    It should used at the beginning of a request handler to avoid fetching the same
    resources and extracted text multiple times.

    Makes sure to clean the caches at the end of the context manager.
    >>> with request_caches():
    ...     resource = await get_resource(kbid, uuid)
    ...     extracted_text = await get_extracted_text_from_field_id(kbid, rid, field_id)
    """
    set_resource_cache()
    set_extracted_text_cache()
    try:
        yield
    finally:
        clear_resource_cache()
        clear_extracted_text_cache()
