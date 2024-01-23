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

from nucliadb_protos.knowledgebox_pb2 import (
    DeletedEntitiesGroups,
    EntitiesGroup,
    EntitiesGroupSummary,
    Entity,
)
from nucliadb_protos.nodereader_pb2 import (
    RelationNodeFilter,
    RelationPrefixSearchRequest,
    RelationSearchRequest,
    RelationSearchResponse,
    TypeList,
)
from nucliadb_protos.noderesources_pb2 import ShardId
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import GetEntitiesResponse

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import (
    AlreadyExists,
    EntitiesGroupNotFound,
    NodeError,
)
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.entities import (
    KB_DELETED_ENTITIES_GROUPS,
    KB_ENTITIES,
    KB_ENTITIES_GROUP,
    EntitiesDataManager,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.settings import settings
from nucliadb_telemetry import errors

from .exceptions import EntityManagementException

MAX_DUPLICATES = 300
MAX_DELETED = 300


class EntitiesManager:
    def __init__(
        self,
        knowledgebox: KnowledgeBox,
        txn: Transaction,
    ):
        self.kb = knowledgebox
        self.txn = txn
        self.kbid = self.kb.kbid

    async def create_entities_group(self, group: str, entities: EntitiesGroup):
        if await self.entities_group_exists(group):
            raise AlreadyExists(f"Entities group {group} already exists")

        await self.store_entities_group(group, entities)

    async def get_entities(self, entities: GetEntitiesResponse):
        async for group, eg in self.iterate_entities_groups(exclude_deleted=True):
            entities.groups[group].CopyFrom(eg)

    async def get_entities_group(self, group: str) -> Optional[EntitiesGroup]:
        deleted = await self.is_entities_group_deleted(group)
        if deleted:
            return None
        return await self.get_entities_group_inner(group)

    async def get_entities_groups(self) -> dict[str, EntitiesGroup]:
        groups = {}
        async for group, eg in self.iterate_entities_groups(exclude_deleted=True):
            groups[group] = eg
        return groups

    async def list_entities_groups(self) -> dict[str, EntitiesGroupSummary]:
        groups = {}
        async for group in self.iterate_entities_groups_names(exclude_deleted=True):
            stored = await self.get_stored_entities_group(group)
            if stored is not None:
                groups[group] = EntitiesGroupSummary(
                    title=stored.title, color=stored.color, custom=stored.custom
                )
            else:
                # We don't want to search for each indexed group, as we are
                # providing a quick summary
                groups[group] = EntitiesGroupSummary()
        return groups

    async def update_entities(self, group: str, entities: dict[str, Entity]):
        """Update entities on an entity group. New entities are appended and existing
        are overwriten. Existing entities not appearing in `entities` are left
        intact. Use `delete_entities` to delete them instead.

        """
        if not await self.entities_group_exists(group):
            raise EntitiesGroupNotFound(f"Entities group '{group}' doesn't exist")

        entities_group = await self.get_stored_entities_group(group)
        if entities_group is None:
            entities_group = EntitiesGroup()

        for name, entity in entities.items():
            entities_group.entities[name].CopyFrom(entity)

        await self.store_entities_group(group, entities_group)

    async def set_entities_group(self, group: str, entities: EntitiesGroup):
        indexed = await self.get_indexed_entities_group(group)
        if indexed is None:
            updated = entities
        else:
            updated = EntitiesGroup()
            updated.CopyFrom(entities)

            for name, entity in indexed.entities.items():
                if name not in updated.entities:
                    updated.entities[name].CopyFrom(entity)
                    updated.entities[name].deleted = True

        await self.store_entities_group(group, updated)

    async def set_entities_group_force(self, group: str, entitiesgroup: EntitiesGroup):
        await self.store_entities_group(group, entitiesgroup)

    async def set_entities_group_metadata(
        self, group: str, *, title: Optional[str] = None, color: Optional[str] = None
    ):
        entities_group = await self.get_stored_entities_group(group)
        if entities_group is None:
            entities_group = EntitiesGroup()

        if title:
            entities_group.title = title
        if color:
            entities_group.color = color

        await self.store_entities_group(group, entities_group)

    async def delete_entities(self, group: str, delete: list[str]):
        stored = await self.get_stored_entities_group(group)

        stored = stored or EntitiesGroup()
        for name in delete:
            if name not in stored.entities:
                entity = stored.entities[name]
                entity.value = name
            else:
                entity = stored.entities[name]
            entity.deleted = True
        await self.store_entities_group(group, stored)

    async def delete_entities_group(self, group: str):
        await self.delete_stored_entities_group(group)
        await self.mark_entities_group_as_deleted(group)

    # Private API

    async def get_entities_group_inner(self, group: str) -> Optional[EntitiesGroup]:
        stored = await self.get_stored_entities_group(group)
        indexed = await self.get_indexed_entities_group(group)
        if stored is None and indexed is None:
            # Entity group does not exist
            return None
        elif stored is not None and indexed is not None:
            entities_group = self.merge_entities_groups(indexed, stored)
        else:
            entities_group = stored or indexed  # type: ignore
        return entities_group

    async def get_stored_entities_group(self, group: str) -> Optional[EntitiesGroup]:
        return await EntitiesDataManager.get_entities_group(self.kbid, group, self.txn)

    async def get_indexed_entities_group(self, group: str) -> Optional[EntitiesGroup]:
        shard_manager = get_shard_manager()

        async def do_entities_search(
            node: AbstractIndexNode, shard_id: str, node_id: str
        ) -> RelationSearchResponse:
            request = RelationSearchRequest(
                shard_id=shard_id,
                prefix=RelationPrefixSearchRequest(
                    prefix="",
                    node_filters=[
                        RelationNodeFilter(
                            node_type=RelationNode.NodeType.ENTITY, node_subtype=group
                        )
                    ],
                ),
            )
            return await node.reader.RelationSearch(request)  # type: ignore

        results = await shard_manager.apply_for_all_shards(
            self.kbid, do_entities_search, settings.relation_search_timeout
        )
        for result in results:
            if isinstance(result, Exception):
                errors.capture_exception(result)
                raise NodeError("Error while querying relation index")

        entities = {}
        for result in results:
            entities.update(
                {node.value: Entity(value=node.value) for node in result.prefix.nodes}
            )

        if not entities:
            return None
        eg = EntitiesGroup(entities=entities)
        return eg

    async def get_deleted_entities_groups(self) -> set[str]:
        deleted: set[str] = set()
        key = KB_DELETED_ENTITIES_GROUPS.format(kbid=self.kbid)
        payload = await self.txn.get(key)
        if payload:
            deg = DeletedEntitiesGroups()
            deg.ParseFromString(payload)
            deleted.update(deg.entities_groups)
        return deleted

    async def entities_group_exists(self, group: str) -> bool:
        stored = await self.get_stored_entities_group(group)
        if stored is not None:
            return True

        indexed = await self.get_indexed_entities_group(group)
        if indexed is not None:
            return True

        return False

    async def iterate_entities_groups(
        self, exclude_deleted: bool
    ) -> AsyncGenerator[tuple[str, EntitiesGroup], None]:
        async for group in self.iterate_entities_groups_names(exclude_deleted):
            eg = await self.get_entities_group_inner(group)
            if eg is None:
                continue
            yield group, eg

    async def iterate_entities_groups_names(
        self, exclude_deleted: bool
    ) -> AsyncGenerator[str, None]:
        if exclude_deleted:
            deleted_groups = await self.get_deleted_entities_groups()

        visited_groups = set()

        # stored groups
        entities_key = KB_ENTITIES.format(kbid=self.kbid)
        async for key in self.txn.keys(entities_key, count=-1):
            group = key.split("/")[-1]
            if exclude_deleted and group in deleted_groups:
                continue
            yield group
            visited_groups.add(group)

        # indexed groups
        indexed_groups = await self.get_indexed_entities_groups_names()
        for group in indexed_groups:
            if (exclude_deleted and group in deleted_groups) or group in visited_groups:
                continue
            yield group
            visited_groups.add(group)

    async def get_indexed_entities_groups_names(self) -> set[str]:
        shard_manager = get_shard_manager()

        async def query_indexed_entities_group_names(
            node: AbstractIndexNode, shard_id: str, node_id: str
        ) -> TypeList:
            return await node.reader.RelationTypes(ShardId(id=shard_id))  # type: ignore

        results = await shard_manager.apply_for_all_shards(
            self.kbid,
            query_indexed_entities_group_names,
            settings.relation_types_timeout,
        )
        for result in results:
            if isinstance(result, Exception):
                errors.capture_exception(result)
                raise NodeError("Error while looking for relations types")

        indexed_groups = set()
        for relation_types in results:
            for item in relation_types.list:
                if item.with_type != RelationNode.NodeType.ENTITY:
                    continue
                group = item.with_subtype
                indexed_groups.add(group)
        return indexed_groups

    async def store_entities_group(self, group: str, eg: EntitiesGroup):
        meta_cache = await EntitiesDataManager.get_entities_meta_cache(
            self.kbid, self.txn
        )
        duplicates = {}
        deleted = []
        duplicate_count = 0
        for entity in eg.entities.values():
            if entity.deleted:
                deleted.append(entity.value)
                continue
            if len(entity.represents) == 0:
                continue
            duplicates[entity.value] = list(entity.represents)
            duplicate_count += len(duplicates[entity.value])

        if duplicate_count > MAX_DUPLICATES:
            raise EntityManagementException(
                f"Too many duplicates: {duplicate_count}. Max of {MAX_DUPLICATES} currently allowed"
            )
        if len(deleted) > MAX_DELETED:
            raise EntityManagementException(
                f"Too many deleted entities: {len(deleted)}. Max of {MAX_DELETED} currently allowed"
            )

        meta_cache.set_duplicates(group, duplicates)
        meta_cache.set_deleted(group, deleted)
        await EntitiesDataManager.set_entities_meta_cache(
            self.kbid, meta_cache, self.txn
        )

        await EntitiesDataManager.set_entities_group(self.kbid, group, eg, self.txn)
        # if it was preivously deleted, we must unmark it
        await self.unmark_entities_group_as_deleted(group)

    async def is_entities_group_deleted(self, group: str):
        deleted_groups = await self.get_deleted_entities_groups()
        return group in deleted_groups

    async def delete_stored_entities_group(self, group: str):
        entities_key = KB_ENTITIES_GROUP.format(kbid=self.kbid, id=group)
        await self.txn.delete(entities_key)

    async def mark_entities_group_as_deleted(self, group: str):
        await EntitiesDataManager.mark_group_as_deleted(self.kbid, group, self.txn)

    async def unmark_entities_group_as_deleted(self, group: str):
        await EntitiesDataManager.unmark_group_as_deleted(self.kbid, group, self.txn)

    @staticmethod
    def merge_entities_groups(indexed: EntitiesGroup, stored: EntitiesGroup):
        """Create a new EntitiesGroup with the merged entities from `stored` and
        `indexed`. The values of `stored` take priority when `stored` and
        `indexed` share entities. That's also true for common fields.

        """
        merged_entities: dict[str, Entity] = {}
        merged_entities.update(indexed.entities)
        merged_entities.update(stored.entities)

        for entity, edata in list(stored.entities.items()):
            # filter out deleted entities
            if edata.deleted:
                del merged_entities[entity]

        merged = EntitiesGroup(
            entities=merged_entities,
            title=stored.title or indexed.title or "",
            color=stored.color or indexed.color or "",
            custom=False,  # if there are indexed entities, can't be a custom group
        )
        return merged
