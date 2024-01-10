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
from typing import Optional

import pytest

PLATFORM_GROUP = "platform"
DEVELOPERS_GROUP = "developers"


@pytest.fixture(scope="function")
async def resource_with_security(nucliadb_writer, knowledgebox):
    kbid = knowledgebox
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


@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_security_is_returned_serialization(
    nucliadb_reader, knowledgebox, resource_with_security
):
    kbid = knowledgebox
    resource_id = resource_with_security

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resource/{resource_id}", params={"show": ["security"]}
    )
    assert resp.status_code == 200, resp.text
    resource = resp.json()
    assert resource["security"]["access_groups"] == [PLATFORM_GROUP, DEVELOPERS_GROUP]


@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize(
    "search_endpoint", ("find_get", "find_post", "search_get", "search_post")
)
async def test_resource_security_search(
    nucliadb_reader, knowledgebox, resource_with_security, search_endpoint
):
    kbid = knowledgebox
    resource_id = resource_with_security

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


async def _test_search_request_with_security(
    search_endpoint: str,
    nucliadb_reader,
    kbid: str,
    query: str,
    security_groups: Optional[list[str]],
    expected_resources: list[str],
):
    payload = {
        "query": query,
    }
    if security_groups:
        payload["security"] = {"groups": security_groups}

    params = {
        "query": query,
    }
    if security_groups:
        params["security_groups"] = security_groups

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
    else:
        raise ValueError(f"Unknown search endpoint: {search_endpoint}")

    assert resp.status_code == 200, resp.text
    search_response = resp.json()
    assert len(search_response["resources"]) == len(expected_resources)
    assert set(search_response["resources"]) == set(expected_resources)
