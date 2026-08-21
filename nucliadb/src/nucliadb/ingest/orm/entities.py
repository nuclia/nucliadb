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

from collections.abc import AsyncGenerator

from nidx_protos.nodereader_pb2 import (
    Faceted,
    GraphSearchRequest,
    SearchRequest,
)

from nucliadb.common.maindb.driver import Transaction
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.settings import settings
from nucliadb.search.requesters.utils import Method, nidx_query
from nucliadb_protos.knowledgebox_pb2 import (
    EntitiesGroup,
    EntitiesGroupSummary,
    Entity,
)
from nucliadb_protos.utils_pb2 import RelationNode
from nucliadb_protos.writer_pb2 import GetEntitiesResponse

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

    async def get_entities(self, entities: GetEntitiesResponse):
        async for group, eg in self.iterate_entities_groups(exclude_deleted=True):
            entities.groups[group].CopyFrom(eg)

    async def get_entities_group(self, group: str) -> EntitiesGroup | None:
        return await self.get_entities_group_inner(group)

    async def get_entities_groups(self) -> dict[str, EntitiesGroup]:
        groups = {}
        async for group, eg in self.iterate_entities_groups(exclude_deleted=True):
            groups[group] = eg
        return groups

    async def list_entities_groups(self) -> dict[str, EntitiesGroupSummary]:
        groups = {}

        async for group in self.iterate_entities_groups_names(exclude_deleted=True):
            groups[group] = EntitiesGroupSummary()

        return groups

    # Private API

    async def get_entities_group_inner(self, group: str) -> EntitiesGroup | None:
        return await self.get_indexed_entities_group(group)

    async def get_indexed_entities_group(self, group: str) -> EntitiesGroup | None:

        # Entities search
        request = GraphSearchRequest()
        # XXX: this is a wild guess. Are those enough or too many?
        request.top_k = 500
        request.kind = GraphSearchRequest.QueryKind.NODES
        request.query.path.path.source.node_type = RelationNode.NodeType.ENTITY
        request.query.path.path.source.node_subtype = group
        request.query.path.path.undirected = True

        result = await nidx_query(
            self.kbid, Method.GRAPH, request, timeout=settings.relation_search_timeout
        )

        entities = {}
        entities.update({node.value: Entity(value=node.value) for node in result.nodes})

        if not entities:
            return None
        eg = EntitiesGroup(entities=entities)
        return eg

    async def iterate_entities_groups(
        self, exclude_deleted: bool
    ) -> AsyncGenerator[tuple[str, EntitiesGroup], None]:
        async for group in self.iterate_entities_groups_names(exclude_deleted):
            eg = await self.get_entities_group_inner(group)
            if eg is None:
                continue
            yield group, eg

    async def iterate_entities_groups_names(
        self,
        exclude_deleted: bool,
    ) -> AsyncGenerator[str, None]:
        visited_groups = set()
        indexed_groups = await self.get_indexed_entities_groups_names()
        for group in indexed_groups:
            if group in visited_groups:
                continue
            yield group
            visited_groups.add(group)

    async def get_indexed_entities_groups_names(
        self,
    ) -> set[str]:
        # search all relation types
        request = SearchRequest(
            result_per_page=0,
            body="",
            document=True,
            paragraph=False,
            faceted=Faceted(labels=["/e"]),
        )
        response = await nidx_query(
            self.kbid, Method.SEARCH, request, timeout=settings.relation_types_timeout
        )
        try:
            facetresults = response.document.facets["/e"].facetresults
        except KeyError:
            # No entities found
            return set()
        else:
            return {facet.tag.split("/")[-1] for facet in facetresults}
