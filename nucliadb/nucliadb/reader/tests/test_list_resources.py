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
from typing import Callable, Optional

import pytest
from httpx import AsyncClient

from nucliadb.reader.api import DEFAULT_RESOURCE_LIST_PAGE_SIZE
from nucliadb.reader.api.v1.router import KB_PREFIX
from nucliadb_models.resource import NucliaDBRoles

# All this scenarios are meant to match a total of 10 resources
# coming from test_pagination_resources. Tests uses redis so order
# is not guaranteed
PAGINATION_TEST_SCENARIOS = [
    (None, None, 10, True),  # Get first (also last) page with default values
    (0, 20, 10, True),  # Get first (also last)page explicitly
    (1, 20, 0, True),  # Get invalid page
    (0, 5, 5, False),  # Get first non-last page
    (1, 5, 5, True),  # Get last full page
    (1, 6, 4, True),  # Get last partial page
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "page, size, expected_resources_count, expected_is_last_page",
    PAGINATION_TEST_SCENARIOS,
)
async def test_list_resources(
    reader_api: Callable[..., AsyncClient],
    test_pagination_resources: str,
    page: Optional[int],
    size: Optional[int],
    expected_resources_count: int,
    expected_is_last_page: bool,
) -> None:
    kbid = test_pagination_resources

    query_params = {}
    if page is not None:
        query_params["page"] = page

    if size is not None:
        query_params["size"] = size

    async with reader_api(roles=[NucliaDBRoles.READER]) as client:
        resp = await client.get(f"/{KB_PREFIX}/{kbid}/resources", params=query_params)
        assert resp.status_code == 200
        resources = resp.json()["resources"]
        pagination = resp.json()["pagination"]

        assert len(resources) == expected_resources_count
        assert pagination["last"] == expected_is_last_page
        assert pagination["page"] == query_params.get("page", 0)
        assert pagination["size"] == query_params.get(
            "size", DEFAULT_RESOURCE_LIST_PAGE_SIZE
        )
