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
import json
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.cache import KB_COUNTER_CACHE


@pytest.mark.asyncio
async def test_kb_counters(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    kbid = test_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/counters",
        )
        assert resp.status_code == 200

        data = resp.json()
        assert data["resources"] == 1
        assert data["paragraphs"] == 2
        assert data["fields"] == 3
        assert data["sentences"] == 3


@pytest.mark.asyncio
async def test_kb_counters_cached(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    from nucliadb_utils.utilities import get_cache

    kbid = test_search_resource

    cache = await get_cache()
    assert cache is not None

    await cache.set(
        KB_COUNTER_CACHE.format(kbid=kbid),
        json.dumps(
            {"resources": 100, "paragraphs": 100, "fields": 100, "sentences": 100}
        ),
    )

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/counters",
        )
        assert resp.status_code == 200

        data = resp.json()
        assert data["resources"] == 100
        assert data["paragraphs"] == 100
        assert data["fields"] == 100
        assert data["sentences"] == 100
        assert "shards" not in data


@pytest.mark.asyncio
async def test_kb_counters_cached_nodebug(
    search_api: Callable[..., AsyncClient], test_search_resource: str
) -> None:
    from nucliadb_utils.utilities import get_cache

    kbid = test_search_resource

    cache = await get_cache()
    assert cache is not None

    await cache.set(
        KB_COUNTER_CACHE.format(kbid=kbid),
        json.dumps(
            {
                "resources": 100,
                "paragraphs": 100,
                "fields": 100,
                "sentences": 100,
                "shards": [],
            }
        ),
    )

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/counters",
        )
        assert resp.status_code == 200

        data = resp.json()
        assert data["resources"] == 100
        assert data["paragraphs"] == 100
        assert data["fields"] == 100
        assert data["sentences"] == 100
        assert "shards" not in data
