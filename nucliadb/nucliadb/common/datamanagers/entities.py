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

import pickle
from typing import AsyncGenerator, Optional

from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import knowledgebox_pb2 as kb_pb2

KB_ENTITIES = "/kbs/{kbid}/entities/"
KB_ENTITIES_GROUP = "/kbs/{kbid}/entities/{id}"
KB_DELETED_ENTITIES_GROUPS = "/kbs/{kbid}/deletedentities"
KB_ENTITIES_CACHE = "/kbs/{kbid}/entities-cache"


class EntitiesMetaCache:
    """
    A cache of duplicates and deleted entities. This is used to speed up
    lookups of duplicate and deletions at query time. It's not used for anything else.

    This is materialized on every change to an entity group.

    [XXX] We're in python pickle hell here. We need to make sure we don't
    change the structure of this class or we'll break the index.
    """

    def __init__(self):
        self.deleted_entities: dict[str, list[str]] = {}
        self.duplicate_entities: dict[str, dict[str, list[str]]] = {}
        # materialize by value for faster lookups
        self.duplicate_entities_by_value: dict[str, dict[str, str]] = {}

    def set_duplicates(self, group_id: str, dups: dict[str, list[str]]) -> None:
        self.duplicate_entities[group_id] = dups
        self.duplicate_entities_by_value[group_id] = {}
        for entity_id, duplicates in dups.items():
            for duplicate in duplicates:
                self.duplicate_entities_by_value[group_id][duplicate] = entity_id

    def set_deleted(self, group_id: str, deleted_entities: list[str]) -> None:
        if len(deleted_entities) == 0:
            self.deleted_entities.pop(group_id, None)
        else:
            self.deleted_entities[group_id] = deleted_entities


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

    @classmethod
    async def get_deleted_groups(
        cls, kbid: str, txn: Transaction
    ) -> kb_pb2.DeletedEntitiesGroups:
        deleted_groups_key = KB_DELETED_ENTITIES_GROUPS.format(kbid=kbid)
        payload = await txn.get(deleted_groups_key)
        deg = kb_pb2.DeletedEntitiesGroups()
        if payload:
            deg.ParseFromString(payload)
        return deg

    @classmethod
    async def mark_group_as_deleted(
        cls, kbid: str, group: str, txn: Transaction
    ) -> None:
        deg = await cls.get_deleted_groups(kbid, txn)
        if group not in deg.entities_groups:
            deg.entities_groups.append(group)
            await txn.set(
                KB_DELETED_ENTITIES_GROUPS.format(kbid=kbid), deg.SerializeToString()
            )

    @classmethod
    async def unmark_group_as_deleted(
        cls, kbid: str, group: str, txn: Transaction
    ) -> None:
        deg = await cls.get_deleted_groups(kbid, txn)
        if group in deg.entities_groups:
            deg.entities_groups.remove(group)
            await txn.set(
                KB_DELETED_ENTITIES_GROUPS.format(kbid=kbid), deg.SerializeToString()
            )

    @classmethod
    async def get_entities_meta_cache(
        cls, kbid: str, txn: Transaction
    ) -> EntitiesMetaCache:
        value = await txn.get(KB_ENTITIES_CACHE.format(kbid=kbid))
        if not value:
            return EntitiesMetaCache()
        return pickle.loads(value)

    @classmethod
    async def set_entities_meta_cache(
        cls, kbid: str, cache: EntitiesMetaCache, txn: Transaction
    ) -> None:
        await txn.set(
            KB_ENTITIES_CACHE.format(kbid=kbid), pickle.dumps(cache, protocol=5)
        )
