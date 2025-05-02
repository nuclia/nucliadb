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
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Generic, Optional, TypeVar, Union

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb_models.resource import Resource

# execution context (filled while running the plan)


@dataclass
class ExecutionContext:
    # nidx search failed on some shards and results aren't from the whole corpus
    nidx_incomplete: bool = False

    # nidx shards queried during retrieval
    nidx_queried_shards: Optional[list[str]] = None

    # whether a query with a greater top_k would have returned more results
    next_page: bool = False

    # number of keyword results obtained from the index
    keyword_result_count: int = 0

    # list of exact matches during keyword search
    keyword_exact_matches: Optional[list[str]] = None


# query plan step outputs


@dataclass
class IndexResult:
    keyword: list[TextBlockMatch]
    semantic: list[TextBlockMatch]
    graph: list[TextBlockMatch]


@dataclass
class BestMatchesHydrated:
    best_text_blocks: list[TextBlockMatch]
    resources: list[Resource]
    best_matches: list[str]


# query plan step logic


T = TypeVar("T")


class PlanStep(ABC, Generic[T]):
    @abstractmethod
    async def execute(self, context: ExecutionContext) -> T:
        pass

    @abstractmethod
    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {self.__class__.__name__: "(explain not implemented)"}


class Plan(Generic[T]):
    def __init__(self, step: PlanStep[T]):
        self._context = ExecutionContext()
        self.step = step

    async def execute(self) -> T:
        return await self.step.execute(self._context)

    def explain(self) -> None:
        def _print_plan(plan: Union[str, Union[str, dict]], offset: int = 0):
            if isinstance(plan, dict):
                for key, value in plan.items():
                    print(" " * offset, "-", key)
                    _print_plan(value, offset + 2)
            else:
                print(" " * offset, "-", plan)

        plan = self.step.explain()
        _print_plan(plan)

    @property
    def context(self) -> ExecutionContext:
        return self._context
