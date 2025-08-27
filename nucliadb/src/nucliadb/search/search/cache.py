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
from typing import Optional

import backoff

from nucliadb.common.cache import (
    extracted_text_cache,
    get_extracted_text_cache,
    get_resource_cache,
    resource_cache,
)
from nucliadb.common.ids import FieldId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.search import SERVICE_NAME
from nucliadb_protos.utils_pb2 import ExtractedText
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)


async def get_resource(kbid: str, uuid: str) -> Optional[ResourceORM]:
    """
    Will try to get the resource from the cache, if it's not there it will fetch it from the ORM and cache it.
    """
    resource_cache = get_resource_cache()
    if resource_cache is None:
        logger.warning("Resource cache not set")
        return await _orm_get_resource(kbid, uuid)

    return await resource_cache.get(kbid, uuid)


async def _orm_get_resource(kbid: str, uuid: str) -> Optional[ResourceORM]:
    async with get_driver().ro_transaction() as txn:
        storage = await get_storage(service_name=SERVICE_NAME)
        kb = KnowledgeBoxORM(txn, storage, kbid)
        return await kb.get(uuid)


async def get_field_extracted_text(field: Field) -> Optional[ExtractedText]:
    if field.extracted_text is not None:
        return field.extracted_text

    cache = get_extracted_text_cache()
    if cache is None:
        logger.warning("Extracted text cache not set")
        return await field.get_extracted_text()

    extracted_text = await cache.get(field.kbid, FieldId(field.uuid, field.type, field.id))
    field.extracted_text = extracted_text
    return extracted_text


@backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
async def field_get_extracted_text(field: Field) -> Optional[ExtractedText]:
    try:
        return await field.get_extracted_text()
    except Exception:
        logger.warning(
            "Error getting extracted text for field. Retrying",
            exc_info=True,
            extra={
                "kbid": field.kbid,
                "resource_id": field.resource.uuid,
                "field": f"{field.type}/{field.id}",
            },
        )
        raise


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

    # This cache size is an arbitrary number, once we have a metric in place and
    # we analyze memory consumption, we can adjust it with more knoweldge
    cache_size = 50
    with resource_cache(cache_size), extracted_text_cache(cache_size):
        yield
