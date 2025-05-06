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

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.search.search.rank_fusion import IndexSource, RankFusionAlgorithm

from .models import ExecutionContext, IndexResult, PlanStep


class RankFusion(PlanStep):
    def __init__(self, algorithm: RankFusionAlgorithm, source: PlanStep[IndexResult]):
        self.algorithm = algorithm
        self.source = source

    async def execute(self, context: ExecutionContext) -> list[TextBlockMatch]:
        index_results = await self.source.execute(context)
        merged = self.algorithm.fuse(
            {
                IndexSource.KEYWORD: index_results.keyword,
                IndexSource.SEMANTIC: index_results.semantic,
                IndexSource.GRAPH: index_results.graph,
            }
        )
        return merged

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "RankFusion": self.source.explain(),
        }
