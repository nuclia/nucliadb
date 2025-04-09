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

import contextlib
import logging
from contextvars import ContextVar
from typing import Optional

from nucliadb.common.cache import (
    ExtractedTextCache,
    delete_extracted_text_cache,
    delete_resource_cache,
    get_extracted_text_cache,
    get_resource_cache,
    set_extracted_text_cache,
    set_resource_cache,
)
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

etcache: ContextVar[Optional[ExtractedTextCache]] = ContextVar("etcache", default=None)


RESOURCE_CACHE_OPS = metrics.Counter("nucliadb_resource_cache_ops", labels={"type": ""})
EXTRACTED_CACHE_OPS = metrics.Counter("nucliadb_extracted_text_cache_ops", labels={"type": ""})


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

    async with resource_cache.get_lock(uuid):
        if not resource_cache.contains(uuid):
            RESOURCE_CACHE_OPS.inc({"type": "miss"})
            orm_resource = await _orm_get_resource(kbid, uuid)
        else:
            RESOURCE_CACHE_OPS.inc({"type": "hit"})

        if orm_resource is not None:
            resource_cache.set(uuid, orm_resource)
        else:
            orm_resource = resource_cache.get(uuid)

    return orm_resource


async def _orm_get_resource(kbid: str, uuid: str) -> Optional[ResourceORM]:
    async with get_driver().transaction(read_only=True) as txn:
        storage = await get_storage(service_name=SERVICE_NAME)
        kb = KnowledgeBoxORM(txn, storage, kbid)
        return await kb.get(uuid)


async def get_field_extracted_text(field: Field) -> Optional[ExtractedText]:
    cache = get_extracted_text_cache()
    if cache is None:
        logger.warning("Extracted text cache not set")
        EXTRACTED_CACHE_OPS.inc({"type": "miss"})
        return await field.get_extracted_text()

    key = f"{field.kbid}/{field.uuid}/{field.id}"
    extracted_text = cache.get(key)
    if extracted_text is not None:
        EXTRACTED_CACHE_OPS.inc({"type": "hit"})
        return extracted_text

    async with cache.get_lock(key):
        # Check again in case another task already fetched it
        extracted_text = cache.get(key)
        if extracted_text is not None:
            EXTRACTED_CACHE_OPS.inc({"type": "hit"})
            return extracted_text

        EXTRACTED_CACHE_OPS.inc({"type": "miss"})
        extracted_text = await field.get_extracted_text()
        if extracted_text is not None:
            # Only cache if we actually have extracted text
            cache.set(key, extracted_text)
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
        delete_resource_cache()
        delete_extracted_text_cache()
