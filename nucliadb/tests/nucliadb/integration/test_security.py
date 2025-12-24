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


@pytest.mark.deploy_modes("standalone")
async def test_resource_security_is_returned_serialization(
    nucliadb_reader: AsyncClient, standalone_knowledgebox: str, resource_with_security
):
    kbid = standalone_knowledgebox
    resource_id = resource_with_security

    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{resource_id}", params={"show": ["security"]})
    assert resp.status_code == 200, resp.text
    resource = resp.json()
    assert set(resource["security"]["access_groups"]) == {PLATFORM_GROUP, DEVELOPERS_GROUP}


@pytest.mark.deploy_modes("standalone")
async def test_resource_security_is_updated(
    nucliadb_reader: AsyncClient, nucliadb_writer, standalone_knowledgebox: str, resource_with_security
):
    kbid = standalone_knowledgebox
    resource_id = resource_with_security

    # Update the security of the resource: make it public for all groups
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{resource_id}",
        json={
            "security": {
                "access_groups": [],
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Check that it was updated properly
    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{resource_id}",
        params={"show": ["security"]},
    )
    assert resp.status_code == 200, resp.text
    resource = resp.json()
    assert resource["security"]["access_groups"] == []


@pytest.mark.parametrize("search_endpoint", ("find_get", "find_post", "search_get", "search_post"))
@pytest.mark.deploy_modes("standalone")
async def test_resource_security_search(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
    search_endpoint,
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
    await _test_search_request_with_security(
        search_endpoint,
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
        await _test_search_request_with_security(
            search_endpoint,
            nucliadb_reader,
            kbid,
            query="resource",
            security_groups=access_groups,
            expected_resources=[resource_id],
        )

    # Querying with an unknown security group should not return the resource
    await _test_search_request_with_security(
        search_endpoint,
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
    await _test_search_request_with_security(
        search_endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=["blah-blah"],
        expected_resources=[resource_id],
    )


async def _test_search_request_with_security(
    search_endpoint: str,
    nucliadb_reader: AsyncClient,
    kbid: str,
    query: str,
    security_groups: list[str] | None,
    expected_resources: list[str],
):
    payload = {
        "query": query,
    }
    if security_groups:
        payload["security"] = {"groups": security_groups}  # type: ignore

    params = {
        "query": query,
    }
    if security_groups:
        params["security_groups"] = security_groups  # type: ignore

    if search_endpoint == "find_post":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json=payload,
        )
    elif search_endpoint == "find_get":
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/find",
            params=params,
        )
    elif search_endpoint == "search_post":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json=payload,
        )
    elif search_endpoint == "search_get":
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params=params,
        )
    elif search_endpoint == "ask_post":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/ask",
            json=payload,
        )
    else:
        raise ValueError(f"Unknown search endpoint: {search_endpoint}")

    assert resp.status_code == 200, resp.text
    search_response = resp.json()
    assert len(search_response["resources"]) == len(expected_resources)
    assert set(search_response["resources"]) == set(expected_resources)


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
