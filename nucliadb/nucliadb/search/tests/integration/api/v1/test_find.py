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

import pytest
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles


@pytest.mark.asyncio
async def test_find(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f"/{KB_PREFIX}/{kbid}/find?query=own+text",
        )
        assert resp.status_code == 200

        data = resp.json()

        # TODO: uncomment when we have the total stable in tests
        # assert data["total"] == 65

        res = next(iter(data["resources"].values()))
        para = next(iter(res["fields"]["/f/file"]["paragraphs"].values()))
        assert para["position"] == {
            "page_number": 0,
            "index": 0,
            "start": 0,
            "end": 45,
            "start_seconds": [0],
            "end_seconds": [10],
        }
