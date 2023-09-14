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

import backoff

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KB_RESOURCE_SHARD
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM

# These should be refactored
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG, KB_RESOURCE_SLUG_BASE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_protos import noderesources_pb2, writer_pb2
from nucliadb_utils.storages.storage import Storage

KB_MATERIALIZED_RESOURCES_COUNT = "/kbs/{kbid}/materialized/resources/count"


class ResourcesDataManager:
    def __init__(self, driver: Driver, storage: Storage):
        self.driver = driver
        self.storage = storage

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
    async def get_all_resource_slugs(self, kbid: str) -> list[str]:
        all_slugs = []
        async with self.driver.transaction() as txn:
            async for key in txn.keys(
                match=KB_RESOURCE_SLUG_BASE.format(kbid=kbid), count=-1
            ):
                slug = key.split("/")[-1]
                all_slugs.append(slug)
        return all_slugs

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
    async def get_resource_id_from_slug(self, kbid: str, slug: str) -> Optional[str]:
        async with self.driver.transaction() as txn:
            rid = await txn.get(KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug))
        if rid is not None:
            return rid.decode()
        else:
            return None

    async def iterate_resource_ids(self, kbid: str) -> AsyncGenerator[str, None]:
        """
        Currently, the implementation of this is optimizing for reducing
        how long a transaction will be open since the caller controls
        how long each item that is yielded will be processed.
        """
        all_slugs = await self.get_all_resource_slugs(kbid)
        for slug in all_slugs:
            rid = await self.get_resource_id_from_slug(kbid, slug)
            if rid is not None:
                yield rid

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
    async def get_resource_shard_id(self, kbid: str, rid: str) -> Optional[str]:
        async with self.driver.transaction() as txn:
            shard = await txn.get(KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid))
            if shard is not None:
                return shard.decode()
            else:
                return None

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
    async def get_resource(self, kbid: str, rid: str) -> Optional[ResourceORM]:
        """
        Not ideal to return Resource type here but refactoring would
        require a lot of changes.

        At least this isolated that dependency here.
        """
        async with self.driver.transaction() as txn:
            kb_orm = KnowledgeBoxORM(txn, self.storage, kbid)
            return await kb_orm.get(rid)

    @backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
    async def get_resource_index_message(
        self, kbid: str, rid: str
    ) -> Optional[noderesources_pb2.Resource]:
        async with self.driver.transaction() as txn:
            kb_orm = KnowledgeBoxORM(txn, self.storage, kbid)
            res = await kb_orm.get(rid)
            if res is None:
                return None
            return (await res.generate_index_message()).brain

    async def calculate_number_of_resources(self, kbid: str) -> int:
        """
        Calculate the number of resources in a knowledgebox.

        This is usually not very fast at all.

        Long term, we could think about implementing a counter; however,
        right now, a counter would be difficult, require a lot of
        refactoring and not worth much value for the APIs we need
        this feature for.

        Finally, we could also query this data from the node; however,
        it is not the source of truth for the value so it is not ideal
        to move it to the node.
        """
        async with self.driver.transaction() as txn:
            return await txn.count(KB_RESOURCE_SLUG_BASE.format(kbid=kbid))

    async def get_number_of_resources(self, kbid: str) -> int:
        """
        Return cached number of resources in a knowledgebox.
        """
        async with self.driver.transaction() as txn:
            raw_value = await txn.get(KB_MATERIALIZED_RESOURCES_COUNT.format(kbid=kbid))
            if raw_value is None:
                return -1
            return int(raw_value)

    async def set_number_of_resources(self, kbid: str, value: int) -> None:
        async with self.driver.transaction() as txn:
            await txn.set(
                KB_MATERIALIZED_RESOURCES_COUNT.format(kbid=kbid), str(value).encode()
            )
            await txn.commit()

    async def get_broker_message(
        self, kbid: str, rid: str
    ) -> Optional[writer_pb2.BrokerMessage]:
        resource = await self.get_resource(kbid, rid)
        if resource is None:
            return None

        async with self.driver.transaction() as txn:
            resource.disable_vectors = False
            resource.txn = txn
            bm = await resource.generate_broker_message()
            return bm
