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

from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2

KB_ENTITIES = "/kbs/{kbid}/entities"
KB_ENTITIES_GROUP = "/kbs/{kbid}/entities/{id}"
KB_DELETED_ENTITIES_GROUPS = "/kbs/{kbid}/deletedentities"


class EntitiesDataManager:
    def __init__(self, driver: Driver):
        self.driver = driver

    async def get_entities_groups(self, kbid: str) -> kb_pb2.EntitiesGroups:
        kbent = kb_pb2.EntitiesGroups()
        async with self.driver.transaction() as txn:
            async for group in EntitiesDataManager.iterate_entities_groups(kbid, txn):
                eg = await EntitiesDataManager.get_entities_group(kbid, group, txn)
                if eg is None:
                    continue
                kbent.entities_groups[group].CopyFrom(eg)
        return kbent

    async def set_entities_groups(
        self, kbid: str, entities_groups: kb_pb2.EntitiesGroups
    ) -> None:
        async with self.driver.transaction() as txn:
            for group, entities in entities_groups.entities_groups.items():
                await EntitiesDataManager.set_entities_group(kbid, group, entities, txn)
            await txn.commit()

    @classmethod
    async def set_entities_group(
        cls, kbid: str, group_id: str, entities: kb_pb2.EntitiesGroup, txn: Transaction
    ) -> None:
        key = KB_ENTITIES_GROUP.format(kbid=kbid, id=group_id)
        await txn.set(key, entities.SerializeToString())

    @classmethod
    async def iterate_entities_groups(
        cls, kbid: str, txn: Transaction
    ) -> AsyncGenerator[str, None]:
        entities_key = KB_ENTITIES.format(kbid=kbid)
        async for key in txn.keys(entities_key, count=-1):
            group = key.split("/")[-1]
            yield group

    @classmethod
    async def get_entities_group(
        cls, kbid: str, group: str, txn: Transaction
    ) -> Optional[kb_pb2.EntitiesGroup]:
        key = KB_ENTITIES_GROUP.format(kbid=kbid, id=group)
        payload = await txn.get(key)
        if not payload:
            return None
        eg = kb_pb2.EntitiesGroup()
        eg.ParseFromString(payload)
        return eg
