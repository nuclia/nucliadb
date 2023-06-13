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
from typing import Dict, Optional

from lru import LRU  # type: ignore

from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.txn_utils import get_transaction
from nucliadb.search import SERVICE_NAME
from nucliadb_utils.utilities import get_storage

rcache: ContextVar[Optional[Dict[str, ResourceORM]]] = ContextVar(
    "rcache", default=None
)


RESOURCE_LOCKS: Dict[str, asyncio.Lock] = LRU(1000)


def get_resource_cache(clear: bool = False) -> Dict[str, ResourceORM]:
    value: Optional[Dict[str, ResourceORM]] = rcache.get()
    if value is None or clear:
        value = {}
        rcache.set(value)
    return value


async def get_resource_from_cache(
    kbid: str, uuid: str, txn: Optional[Transaction] = None
) -> Optional[ResourceORM]:
    orm_resource: Optional[ResourceORM] = None

    resource_cache = get_resource_cache()

    if uuid not in RESOURCE_LOCKS:
        RESOURCE_LOCKS[uuid] = asyncio.Lock()

    async with RESOURCE_LOCKS[uuid]:
        if uuid not in resource_cache:
            if txn is None:
                txn = await get_transaction()
            storage = await get_storage(service_name=SERVICE_NAME)
            kb = KnowledgeBoxORM(txn, storage, kbid)
            orm_resource = await kb.get(uuid)

        if orm_resource is not None:
            resource_cache[uuid] = orm_resource
        else:
            orm_resource = resource_cache.get(uuid)

    return orm_resource
