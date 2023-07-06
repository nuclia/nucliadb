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

from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KB_RESOURCE_SHARD
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM

# These should be refactored
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG, KB_RESOURCE_SLUG_BASE
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb_protos import noderesources_pb2
from nucliadb_utils.storages.storage import Storage


class ResourcesDataManager:
    def __init__(self, driver: Driver, storage: Storage):
        self.driver = driver
        self.storage = storage

    async def iterate_resource_ids(self, kbid: str) -> AsyncGenerator[str, None]:
        async with self.driver.transaction() as txn:
            keys_generator = txn.keys(
                match=KB_RESOURCE_SLUG_BASE.format(kbid=kbid), count=-1
            )
            async for key in keys_generator:
                slug = key.split("/")[-1]
                rid = await txn.get(KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug))
                if rid is not None:
                    yield rid.decode()

    async def get_resource_shard_id(self, kbid: str, rid: str) -> Optional[str]:
        async with self.driver.transaction() as txn:
            shard = await txn.get(KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid))
            if shard is not None:
                return shard.decode()
            else:
                return None

    async def get_resource(self, kbid: str, rid: str) -> Optional[ResourceORM]:
        """
        Not ideal to return Resource type here but refactoring would
        require a lot of changes.

        At least this isolated that dependency here.
        """
        async with self.driver.transaction() as txn:
            kb_orm = KnowledgeBoxORM(txn, self.storage, kbid)
            return await kb_orm.get(rid)

    async def get_resource_index_message(
        self, kbid: str, rid: str
    ) -> Optional[noderesources_pb2.Resource]:
        async with self.driver.transaction() as txn:
            kb_orm = KnowledgeBoxORM(txn, self.storage, kbid)
            res = await kb_orm.get(rid)
            if res is None:
                return None
            return (await res.generate_index_message()).brain
