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

from contextvars import ContextVar
from typing import Any, AsyncIterator, Dict, Optional

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.txn_utils import get_transaction
from nucliadb.train import SERVICE_NAME
from nucliadb.train.types import TrainBatchType
from nucliadb_utils.utilities import get_storage

rcache: ContextVar[Optional[Dict[str, ResourceORM]]] = ContextVar(
    "rcache", default=None
)


def get_resource_cache(clear: bool = False) -> Dict[str, ResourceORM]:
    value: Optional[Dict[str, ResourceORM]] = rcache.get()
    if value is None or clear:
        value = {}
        rcache.set(value)
    return value


async def get_resource_from_cache_or_db(kbid: str, uuid: str) -> Optional[ResourceORM]:
    resouce_cache = get_resource_cache()
    orm_resource: Optional[ResourceORM] = None
    if uuid not in resouce_cache:
        transaction = await get_transaction()
        storage = await get_storage(service_name=SERVICE_NAME)
        kb = KnowledgeBoxORM(transaction, storage, kbid)
        orm_resource = await kb.get(uuid)
        if orm_resource is not None:
            resouce_cache[uuid] = orm_resource
    else:
        orm_resource = resouce_cache.get(uuid)
    return orm_resource


async def batchify(
    producer: AsyncIterator[Any], size: int, batch_klass: TrainBatchType
):
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
