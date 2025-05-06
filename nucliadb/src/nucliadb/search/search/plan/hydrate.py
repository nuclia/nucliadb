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
import asyncio
from typing import Union

from typing_extensions import Self

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.search import SERVICE_NAME
from nucliadb.search.search.hydrator import (
    ResourceHydrationOptions,
    TextBlockHydrationOptions,
    hydrate_resource_metadata,
    hydrate_text_block,
)
from nucliadb_models.resource import Resource

from .models import ExecutionContext, PlanStep


class HydrateTextBlocks(PlanStep):
    def __init__(
        self,
        kbid: str,
        hydration_options: TextBlockHydrationOptions,
        max_ops: asyncio.Semaphore,
    ):
        self.kbid = kbid
        self.hydration_options = hydration_options
        self.max_ops = max_ops

    def plan(self, uses: PlanStep[list[TextBlockMatch]]) -> Self:
        self.source = uses
        return self

    async def execute(self, context: ExecutionContext) -> list[TextBlockMatch]:
        text_blocks = await self.source.execute(context)

        ops = []
        for text_block in text_blocks:
            ops.append(
                asyncio.create_task(
                    hydrate_text_block(
                        self.kbid,
                        text_block,
                        self.hydration_options,
                        concurrency_control=self.max_ops,
                    )
                )
            )

        # TODO: metrics
        hydrated = await asyncio.gather(*ops)
        return hydrated

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "HydrateTextBlocks": self.source.explain(),
        }


class HydrateResources(PlanStep):
    def __init__(
        self, kbid: str, hydration_options: ResourceHydrationOptions, max_ops: asyncio.Semaphore
    ):
        self.kbid = kbid
        self.hydration_options = hydration_options
        self.max_ops = max_ops

    def plan(self, uses: PlanStep[list[TextBlockMatch]]) -> Self:
        self.source = uses
        return self

    async def execute(self, context: ExecutionContext) -> list[Resource]:
        text_blocks = await self.source.execute(context)

        ops = {}
        for text_block in text_blocks:
            rid = text_block.paragraph_id.rid

            if rid not in ops:
                ops[rid] = asyncio.create_task(
                    hydrate_resource_metadata(
                        self.kbid,
                        rid,
                        options=self.hydration_options,
                        concurrency_control=self.max_ops,
                        service_name=SERVICE_NAME,
                    )
                )
        # TODO: metrics
        hydrated = await asyncio.gather(*ops.values())
        resources = [resource for resource in hydrated if resource is not None]
        return resources

    def explain(self) -> Union[str, dict[str, Union[str, dict]]]:
        return {
            "HydrateResources": self.source.explain(),
        }
