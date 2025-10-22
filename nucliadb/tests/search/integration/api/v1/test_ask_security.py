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

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture


@pytest.mark.deploy_modes("cluster")
@pytest.mark.parametrize(
    "endpoint,ask_module",
    [
        (f"/kb/{{kbid}}/ask", "kb_ask"),
        # These slugs and ids are harcoded on the test_search_resource fixture
        (f"/kb/{{kbid}}/slug/foobar-slug/ask", "resource_ask"),
        (f"/kb/{{kbid}}/resource/68b6e3b747864293b71925b7bacaee7/ask", "resource_ask"),
    ],
)
async def test_ask_receives_injected_security_groups(
    nucliadb_search: AsyncClient,
    test_search_resource: str,
    mocker: MockerFixture,
    endpoint: str,
    ask_module: str,
) -> None:
    from nucliadb.search.api.v1 import ask
    from nucliadb.search.api.v1.resource import ask as resource_ask
    from nucliadb_models.search import AskRequest

    kbid = test_search_resource

    target_module = ask if ask_module == "kb_ask" else resource_ask
    spy = mocker.spy(target_module, "create_ask_response")

    url = endpoint.format(kbid=kbid)

    # Test security groups only on authorizer headers
    resp = await nucliadb_search.post(
        url,
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
    resp = await nucliadb_search.post(
        url,
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
    resp = await nucliadb_search.post(
        url,
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
    resp = await nucliadb_search.post(
        url,
        json={"query": "title"},
    )
    assert resp.status_code == 200
    spy.assert_called_once()
    ask_request = spy.call_args[1]["ask_request"]
    assert isinstance(ask_request, AskRequest)
    assert ask_request.security is None
    spy.reset_mock()
