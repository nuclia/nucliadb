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

import pytest
from httpx import AsyncClient

from nucliadb_protos.resources_pb2 import FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync

PLATFORM_GROUP = "platform"
DEVELOPERS_GROUP = "developers"


@pytest.fixture(scope="function")
async def resource_with_security(
    nucliadb_writer: AsyncClient, standalone_knowledgebox: str, nucliadb_ingest_grpc: WriterStub
):
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
    rid = resp.json()["uuid"]

    # Add a broker message with paragraphs via the grpc interface
    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    bmb.with_title("Test resource")
    bmb.with_summary("My text discussing about something")

    text_field = bmb.field_builder("text1", FieldType.TEXT)
    text_field.add_paragraph("My text discussing about something")

    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid


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


@pytest.mark.parametrize(
    "search_endpoint",
    (
        ("GET", "find"),
        ("POST", "find"),
        ("GET", "search"),
        ("POST", "search"),
        ("GET", "suggest"),
    ),
)
@pytest.mark.deploy_modes("standalone")
async def test_resource_security_search(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
    search_endpoint: tuple[str, str],
):
    kbid = standalone_knowledgebox
    method, endpoint = search_endpoint
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
        method,
        endpoint,
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
            method,
            endpoint,
            nucliadb_reader,
            kbid,
            query="resource",
            security_groups=access_groups,
            expected_resources=[resource_id],
        )

    # Querying with an unknown security group should not return the resource
    await _test_search_request_with_security(
        method,
        endpoint,
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
        method,
        endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=["blah-blah"],
        expected_resources=[resource_id],
    )


async def _test_search_request_with_security(
    method: str,
    endpoint: str,
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

    if method == "POST" and endpoint == "find":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json=payload,
        )
    elif method == "GET" and endpoint == "find":
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/find",
            params=params,
        )
    elif method == "POST" and endpoint == "search":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/search",
            json=payload,
        )
    elif method == "GET" and endpoint == "search":
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/search",
            params=params,
        )
    elif method == "GET" and endpoint == "suggest":
        resp = await nucliadb_reader.get(
            f"/kb/{kbid}/suggest",
            params=params,
        )
    else:
        raise ValueError(f"Unknown method and/or search endpoint: {method} {endpoint}")

    assert resp.status_code == 200, resp.text

    if endpoint in ("search", "find"):
        search_response = resp.json()
        assert len(search_response["resources"]) == len(expected_resources)
        assert set(search_response["resources"]) == set(expected_resources)
    elif endpoint in ("suggest",):
        suggest_response = resp.json()
        resources = [paragraph["rid"] for paragraph in suggest_response["paragraphs"]["results"]]
        assert len(resources) == len(expected_resources)
        assert set(resources) == set(expected_resources)
    else:
        raise ValueError(f"Unknown method and/or search endpoint: {method} {endpoint}")
