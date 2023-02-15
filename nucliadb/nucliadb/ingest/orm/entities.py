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

import random
from typing import AsyncGenerator, List, Optional, Set, Tuple

from nucliadb_protos.knowledgebox_pb2 import (
    DeletedEntitiesGroups,
    EntitiesGroup,
    Entity,
)
from nucliadb_protos.nodereader_pb2 import (
    RelationNodeFilter,
    RelationPrefixSearchRequest,
    RelationSearchRequest,
)
from nucliadb_protos.noderesources_pb2 import ShardId
from nucliadb_protos.nodewriter_pb2 import SetGraph
from nucliadb_protos.utils_pb2 import JoinGraph, JoinGraphEdge, Relation, RelationNode
from nucliadb_protos.writer_pb2 import GetEntitiesGroupResponse, GetEntitiesResponse

from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.maindb.keys import (
    KB_DELETED_ENTITIES_GROUPS,
    KB_ENTITIES,
    KB_ENTITIES_GROUP,
)
from nucliadb.ingest.orm import knowledgebox


class EntitiesManager:
    def __init__(self, knowledgebox: "knowledgebox.Knowledgebox", txn):
        self.kb = knowledgebox
        self.txn = txn
        self.kbid = self.kb.kbid

    async def get_entities(self, entities: GetEntitiesResponse):
        async for group, eg in self.entitiesgroups_iterator(exclude_deleted=True):
            entities.groups[group].CopyFrom(eg)

    async def get_entitiesgroup(
        self, group: str, entitiesgroup: GetEntitiesGroupResponse
    ):
        deleted_groups = await self.get_deleted_entitiesgroups()
        if group in deleted_groups:
            return
        eg = await self.get_entitiesgroup_inner(group)
        if eg is not None:
            entitiesgroup.group.CopyFrom(eg)

    async def set_entities(self, group: str, entities: EntitiesGroup):
        new_entities = EntitiesGroup()
        stored_entities = await self.get_entitiesgroup_inner(group)

        if stored_entities is None:
            new_entities = entities
        else:
            merged = {}

            for key, entity in stored_entities.entities.items():
                if key not in entities.entities:
                    entity.status = Entity.DiffStatus.DELETED
                else:
                    entity.status = entities.entities[key].status
                merged[key] = entity

            # overwrite stored entities with new ones
            merged.update(entities.entities)

            new_entities.MergeFrom(
                EntitiesGroup(
                    entities=merged,
                    title=entities.title,
                    color=entities.color,
                    custom=entities.custom,
                )
            )

        entities_key = KB_ENTITIES_GROUP.format(kbid=self.kbid, id=group)
        await self.txn.set(entities_key, new_entities.SerializeToString())
        # TODO: properly index new entities
        await self.index_entities(group, new_entities)

    async def set_entities_force(self, group: str, entities: EntitiesGroup):
        entities_key = KB_ENTITIES_GROUP.format(kbid=self.kbid, id=group)
        await self.txn.set(entities_key, entities.SerializeToString())

    async def del_entities(self, group: str):
        entities_key = KB_ENTITIES_GROUP.format(kbid=self.kbid, id=group)
        await self.txn.delete(entities_key)

        deleted_groups_key = KB_DELETED_ENTITIES_GROUPS.format(kbid=self.kbid)
        payload = await self.txn.get(deleted_groups_key)
        deg = DeletedEntitiesGroups()
        if payload is not None:
            deg.ParseFromString(payload)
        if group not in deg.entities_groups:
            deg.entities_groups.append(group)
        await self.txn.set(deleted_groups_key, deg.SerializeToString())

    async def entitiesgroups_iterator(
        self, exclude_deleted: bool
    ) -> AsyncGenerator[Tuple[str, EntitiesGroup], None]:
        async for group in self.entitiesgroups_name_iterator(exclude_deleted):
            eg = await self.get_entitiesgroup_inner(group)
            if eg is not None:
                yield group, eg

    async def entitiesgroups_name_iterator(
        self, exclude_deleted: bool
    ) -> AsyncGenerator[str, None]:
        if exclude_deleted:
            deleted_groups = await self.get_deleted_entitiesgroups()
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
        node, shard_id = random.choice(
            [node async for node in self.kb.iterate_kb_nodes()]
        )
        types = await node.reader.RelationTypes(ShardId(id=shard_id))
        for item in types.list:
            if item.with_type != RelationNode.NodeType.ENTITY:
                continue
            group = item.with_subtype
            if (exclude_deleted and group in deleted_groups) or group in visited_groups:
                continue
            yield group
            visited_groups.add(group)

    async def get_deleted_entitiesgroups(self) -> Set[str]:
        deleted: Set[str] = set()
        key = KB_DELETED_ENTITIES_GROUPS.format(kbid=self.kbid)
        payload = await self.txn.get(key)
        if payload is not None:
            deg = DeletedEntitiesGroups()
            deg.ParseFromString(payload)
            deleted.update(deg.entities_groups)
        return deleted

    async def get_entitiesgroup_inner(self, group: str) -> Optional[EntitiesGroup]:
        stored_entities = await self.get_stored_entities(group)
        indexed_entities = await self.get_indexed_entities(group)
        entities = self.merge_entities(stored_entities, indexed_entities)
        return entities

    async def get_stored_entities(self, group: str) -> Optional[EntitiesGroup]:
        key = KB_ENTITIES_GROUP.format(kbid=self.kbid, id=group)
        payload = await self.txn.get(key)
        if payload is None:
            return None

        egd = EntitiesGroup()
        egd.ParseFromString(payload)
        return egd

    async def get_indexed_entities(self, group: str) -> List[str]:
        node, shard_id = random.choice(
            [node async for node in self.kb.iterate_kb_nodes()]
        )
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
        results = await node.reader.RelationSearch(request)

        entities = [node.value for node in results.prefix.nodes]
        return entities

    @staticmethod
    def merge_entities(
        stored: Optional[EntitiesGroup], indexed: List[str]
    ) -> Optional[EntitiesGroup]:
        if not indexed:
            return stored

        title = ""
        color = ""
        custom = False
        entities = {}

        if stored is None:
            custom = False

            for entity in indexed:
                entities[entity] = Entity(
                    value=entity,
                    status=Entity.DiffStatus.NORMAL,
                )
        else:
            title = stored.title
            color = stored.color
            custom = stored.custom

            for name, entity in stored.entities.items():
                # skip entities marked as deleted, as we provide a custom view
                if entity.status != Entity.DiffStatus.DELETED:
                    entities[name] = entity

            for name in indexed:
                if (
                    name in stored.entities
                    and stored.entities[name].status == Entity.DiffStatus.DELETED
                ):
                    continue

                entities.setdefault(
                    name, Entity(value=name, status=Entity.DiffStatus.NORMAL)
                )

        eg = EntitiesGroup(entities=entities, title=title, color=color, custom=custom)
        return eg

    async def index_entities(self, group: str, entities: EntitiesGroup):
        # TODO properly indexing of SYNONYM relations
        nodes = {
            i: RelationNode(
                value=entity.value,
                ntype=RelationNode.NodeType.ENTITY,
                subtype=group,
            )
            for i, (name, entity) in enumerate(entities.entities.items())
        }

        jg = JoinGraph(nodes=nodes, edges=[])

        async for node, shard_id in self.kb.iterate_kb_nodes():
            sg = SetGraph(shard_id=ShardId(id=shard_id), graph=jg)
            await node.writer.JoinGraph(sg)
