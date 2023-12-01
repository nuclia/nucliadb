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
from typing import Dict, Generator, Optional

from lru import LRU  # type: ignore

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.txn_utils import get_transaction
from nucliadb.search import SERVICE_NAME
from nucliadb_telemetry import metrics
from nucliadb_utils.utilities import get_storage

ResourceCacheType = Dict[str, ResourceORM]

logger = logging.getLogger(__name__)

RESOURCE_LOCKS: Dict[str, asyncio.Lock] = LRU(1000)  # type: ignore
RESOURCE_CACHE_OPS = metrics.Counter("nucliadb_resource_cache_ops", labels={"type": ""})


@contextlib.contextmanager
def resource_cache() -> Generator[ResourceCacheType, None, None]:
    rcache: ResourceCacheType = {}
    try:
        yield rcache
    finally:
        for resource in rcache.values():
            resource.clean()
        rcache.clear()


async def get_resource_from_cache_or_db(
    kbid: str, uuid: str, resource_cache: ResourceCacheType
) -> Optional[ResourceORM]:
    orm_resource: Optional[ResourceORM] = None

    if uuid not in RESOURCE_LOCKS:
        RESOURCE_LOCKS[uuid] = asyncio.Lock()

    async with RESOURCE_LOCKS[uuid]:
        if uuid not in resource_cache:
            RESOURCE_CACHE_OPS.inc({"type": "miss"})
            orm_resource = await get_resource_from_db(kbid, uuid)
        else:
            RESOURCE_CACHE_OPS.inc({"type": "hit"})

        if orm_resource is not None:
            resource_cache[uuid] = orm_resource
        else:
            orm_resource = resource_cache.get(uuid)

    return orm_resource


async def get_resource_from_db(kbid: str, uuid: str) -> Optional[ResourceORM]:
    txn = await get_transaction()
    storage = await get_storage(service_name=SERVICE_NAME)
    kb = KnowledgeBoxORM(txn, storage, kbid)
    return await kb.get(uuid)
