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
from typing import Callable

import asyncio
import pytest
import time
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.tests.asyncbenchmark import AsyncBenchmarkFixture


@pytest.mark.benchmark(
    group="search",
    min_time=0.1,
    max_time=0.5,
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=False,
)
@pytest.mark.asyncio
async def test_multiple_fuzzy_search_resource_all(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str, asyncbenchmark: AsyncBenchmarkFixture
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await asyncbenchmark(
            client.get,
            f'/{KB_PREFIX}/{kbid}/search?query=own+test+"This is great"&highlight=true&page_number=0&page_size=20',
        )
        assert resp.status_code == 200

@pytest.mark.benchmark(
    group="search",
    min_time=0.1,
    max_time=0.5,
    min_rounds=5,
    timer=time.time,
    disable_gc=True,
    warmup=False,
)
@pytest.mark.asyncio
async def test_search_resource_all(
    search_api: Callable[..., AsyncClient], test_search_resource: str, asyncbenchmark: AsyncBenchmarkFixture
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        await asyncio.sleep(1)
        resp = await asyncbenchmark(
            client.get,
            f"/{KB_PREFIX}/{kbid}/search?query=own+text&split=true&highlight=true&text_resource=true",
        )
        assert resp.status_code == 200