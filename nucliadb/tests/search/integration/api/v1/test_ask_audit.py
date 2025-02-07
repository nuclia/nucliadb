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

from httpx import AsyncClient
from pytest_mock import MockerFixture

from nucliadb.search.api.v1.router import KB_PREFIX


async def test_ask_receives_injected_security_groups(
    cluster_nucliadb_search: AsyncClient, test_search_resource: str, mocker: MockerFixture
) -> None:
    from nucliadb.search.api.v1 import ask

    kbid = test_search_resource

    spy = mocker.spy(ask, "create_ask_response")

    resp = await cluster_nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        json={"query": "title"},
        headers={"x-nucliadb-security-groups": "group1;group2"},
    )
    assert resp.status_code == 200
    print(spy.call_args)
