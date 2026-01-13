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
import json

import pytest
from httpx import AsyncClient
from pytest_mock import MockerFixture


@pytest.mark.parametrize(
    "endpoint,ask_module",
    [
        (f"/kb/{{kbid}}/ask", "kb_ask"),
        # These slugs and ids are harcoded on the test_search_resource fixture
        (f"/kb/{{kbid}}/slug/foobar-slug/ask", "resource_ask"),
        (f"/kb/{{kbid}}/resource/68b6e3b747864293b71925b7bacaee77/ask", "resource_ask"),
    ],
)
@pytest.mark.deploy_modes("standalone")
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


PLATFORM_GROUP = "platform"
DEVELOPERS_GROUP = "developers"


@pytest.fixture(scope="function")
async def resource_with_security(nucliadb_writer: AsyncClient, standalone_knowledgebox: str):
    kbid = standalone_knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Test resource",
            "texts": {
                "text1": {"body": "My text discussing about something"},
            },
            "security": {
                "access_groups": [PLATFORM_GROUP, DEVELOPERS_GROUP],
            },
        },
    )
    assert resp.status_code == 201, resp.text
    return resp.json()["uuid"]


@pytest.mark.parametrize("ask_endpoint", ("ask_post",))
@pytest.mark.deploy_modes("standalone")
async def test_resource_security_ask(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
    ask_endpoint: str,
):
    kbid = standalone_knowledgebox
    resource_id = resource_with_security
    support_group = "support"
    # Add another group to the resource
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{resource_id}",
        json={
            "security": {
                "access_groups": [PLATFORM_GROUP, DEVELOPERS_GROUP, support_group],
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Querying without security should return the resource
    await _test_ask_request_with_security(
        ask_endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[resource_id],
    )

    # Querying with security groups should return the resource
    for access_groups in (
        [DEVELOPERS_GROUP],
        [PLATFORM_GROUP],
        [support_group],
        [PLATFORM_GROUP, DEVELOPERS_GROUP],
        # Adding an unknown group should not affect the result, as
        # the index is returning the union of results for each group
        [DEVELOPERS_GROUP, "some-unknown-group"],
    ):
        await _test_ask_request_with_security(
            ask_endpoint,
            nucliadb_reader,
            kbid,
            query="resource",
            security_groups=access_groups,
            expected_resources=[resource_id],
        )

    # Querying with an unknown security group should not return the resource
    await _test_ask_request_with_security(
        ask_endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=["some-unknown-group"],
        expected_resources=[],
    )

    # Make it public now
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{resource_id}",
        json={
            "security": {
                "access_groups": [],
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Wait for the tantivy index to be updated. This is expected as we are using
    # the bindings and the tantivy reader is not notified in the same process.
    await asyncio.sleep(1)

    # Querying with an unknown security group should return the resource now, as it is public
    await _test_ask_request_with_security(
        ask_endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=["blah-blah"],
        expected_resources=[resource_id],
    )


async def _test_ask_request_with_security(
    ask_endpoint: str,
    nucliadb_reader,
    kbid: str,
    query: str,
    security_groups: list[str] | None,
    expected_resources: list[str],
):
    payload = {
        "query": query,
    }
    headers = {"x_synchronous": "true"}
    if security_groups:
        payload["security"] = {"groups": security_groups}  # type: ignore

    if ask_endpoint == "ask_post":
        resp = await nucliadb_reader.post(f"/kb/{kbid}/ask", json=payload, headers=headers)
        assert resp.status_code == 200, resp.text

        messages = resp.text.split("\n")
        for message in messages:
            json_message = json.loads(message)
            if json_message["item"]["type"] == "retrieval":
                search_response = json_message["item"]["results"]
                resource_ids = list(search_response["resources"].keys())
                break
    else:
        raise ValueError(f"Unknown search endpoint: {ask_endpoint}")

    assert len(resource_ids) == len(expected_resources)
    assert set(resource_ids) == set(expected_resources)
