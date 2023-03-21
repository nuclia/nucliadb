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
import os
from typing import Callable

import pytest
from httpx import AsyncClient

from nucliadb.search.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles

RUNNING_IN_GH_ACTIONS = os.environ.get("CI", "").lower() == "true"


@pytest.mark.xfail(RUNNING_IN_GH_ACTIONS, reason="Somethimes this fails in GH actions")
@pytest.mark.asyncio
async def test_multiple_fuzzy_search_resource_all(
    search_api: Callable[..., AsyncClient], multiple_search_resource: str
) -> None:
    kbid = multiple_search_resource

    async with search_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(
            f'/{KB_PREFIX}/{kbid}/search?query=own+test+"This is great"&highlight=true&page_number=0&page_size=20',
        )
        if resp.status_code != 200:
            print(resp.content)

        assert resp.status_code == 200
        assert len(resp.json()["paragraphs"]["results"]) == 20

        # Expected results:
        # - 'text' should not be highlighted as we are searching by 'test' in the query
        # - 'This is great' should be highlighted because it is an exact query search
        # - 'own' should be highlighted because it is not a fuzzy result
        assert (
            resp.json()["paragraphs"]["results"][0]["text"]
            == "My <mark>own</mark> text Ramon. <mark>This is great</mark> to be here. "
        )
