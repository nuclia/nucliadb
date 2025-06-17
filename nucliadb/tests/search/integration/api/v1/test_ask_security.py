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
    cluster_nucliadb_search: AsyncClient,
    test_search_resource: str,
    mocker: MockerFixture,
) -> None:
    from nucliadb.search.api.v1 import ask
    from nucliadb_models.search import AskRequest

    kbid = test_search_resource

    spy = mocker.spy(ask, "create_ask_response")

    # Test security groups only on authorizer headers
    resp = await cluster_nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        json={"query": "title"},
        headers={"x-nucliadb-security-groups": "group1;group2"},
    )
    assert resp.status_code == 200
    spy.assert_called_once()
    ask_request = spy.call_args[1]["ask_request"]
    assert isinstance(ask_request, AskRequest)
    assert ask_request.security is not None
    assert set(ask_request.security.groups) == {"group1", "group2"}
    spy.reset_mock()

    # Test security groups only on payload
    resp = await cluster_nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        json={"query": "title", "security": {"groups": ["group1", "group2"]}},
    )
    assert resp.status_code == 200
    spy.assert_called_once()
    ask_request = spy.call_args[1]["ask_request"]
    assert isinstance(ask_request, AskRequest)
    assert ask_request.security is not None
    assert set(ask_request.security.groups) == {"group1", "group2"}
    spy.reset_mock()

    # Test security groups on headers override payload
    resp = await cluster_nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        headers={"x-nucliadb-security-groups": "group1;group2"},
        json={"query": "title", "security": {"groups": ["group3", "group4"]}},
    )
    assert resp.status_code == 200
    spy.assert_called_once()
    ask_request = spy.call_args[1]["ask_request"]
    assert isinstance(ask_request, AskRequest)
    assert ask_request.security is not None
    assert set(ask_request.security.groups) == {"group1", "group2"}
    spy.reset_mock()

    # Test no security groups
    resp = await cluster_nucliadb_search.post(
        f"/{KB_PREFIX}/{kbid}/ask",
        json={"query": "title"},
    )
    assert resp.status_code == 200
    spy.assert_called_once()
    ask_request = spy.call_args[1]["ask_request"]
    assert isinstance(ask_request, AskRequest)
    assert ask_request.security is None
    spy.reset_mock()
