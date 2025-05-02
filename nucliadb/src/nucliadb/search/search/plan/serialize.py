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
from typing import Union

from nidx_protos.nodereader_pb2 import SearchResponse

from nucliadb.search.search.merge import merge_relations_results
from nucliadb.search.search.query_parser.models import UnitRetrieval
from nucliadb_models.search import (
    Relations,
)

from .models import ExecutionContext, PlanStep


class SerializeRelations(PlanStep):
    def __init__(self, retrieval: UnitRetrieval, uses: PlanStep[SearchResponse]):
        self.retrieval = retrieval
        self.proto_query = uses

    async def execute(self, context: ExecutionContext) -> Relations:
        if self.retrieval.query.relation is not None:
            search_response = await self.proto_query.execute(context)
            entry_points = self.retrieval.query.relation.entry_points
            relations = await merge_relations_results([search_response.graph], entry_points)
        else:
            relations = Relations(entities={})

        return relations

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "Cached": self.proto_query.explain(),
        }
