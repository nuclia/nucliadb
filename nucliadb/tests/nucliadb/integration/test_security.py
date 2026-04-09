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


async def _create_resource(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    kbid: str,
    title: str,
    texts: dict[str, str],
    security_groups: list[str] | None = None,
) -> str:
    """Create a resource via the writer API and index it via the grpc interface.
    Returns:
        The resource UUID.
    """
    payload: dict = {
        "title": title,
        "texts": {field_id: {"body": body} for field_id, body in texts.items()},
    }
    if security_groups is not None:
        payload["security"] = {"access_groups": security_groups}

    resp = await nucliadb_writer.post(f"/kb/{kbid}/resources", json=payload)
    assert resp.status_code == 201, resp.text
    rid = resp.json()["uuid"]

    bmb = BrokerMessageBuilder(
        kbid=kbid,
        rid=rid,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )
    bmb.with_title(title)
    # Use the first text field body as the summary
    first_body = next(iter(texts.values()))
    bmb.with_summary(first_body)

    for field_id, body in texts.items():
        text_field = bmb.field_builder(field_id, FieldType.TEXT)
        text_field.add_paragraph(body)

    bm = bmb.build()
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()

    return rid


@pytest.fixture(scope="function")
async def resource_with_security(
    nucliadb_writer: AsyncClient, standalone_knowledgebox: str, nucliadb_ingest_grpc: WriterStub
):
    return await _create_resource(
        nucliadb_writer,
        nucliadb_ingest_grpc,
        kbid=standalone_knowledgebox,
        title="Test resource",
        texts={"text1": "My text discussing about something"},
        security_groups=[PLATFORM_GROUP, DEVELOPERS_GROUP],
    )


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
        ("POST", "suggest"),
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
    params = {
        "query": query,
    }
    if security_groups is not None:
        payload["security"] = {"groups": security_groups}  # type: ignore
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
    elif method == "POST" and endpoint == "suggest":
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/suggest",
            json=payload,
        )
    else:
        raise ValueError(f"Unknown method and/or search endpoint: {method} {endpoint}")

    assert resp.status_code == 200, resp.text
    if endpoint == "search":
        search_response = resp.json()
        # Check paragraph search results
        p_rids = [p_result["rid"] for p_result in search_response["paragraphs"]["results"]]
        assert len(p_rids) == len(expected_resources), "Unexpected number of paragraph results"
        assert set(p_rids) == set(expected_resources), "Unexpected paragraph results"
        # Check texts search results
        t_rids = [t_result["rid"] for t_result in search_response["fulltext"]["results"]]
        assert len(t_rids) == len(expected_resources), "Unexpected number of text results"
        assert set(t_rids) == set(expected_resources), "Unexpected text results"
    elif endpoint == "find":
        find_response = resp.json()
        assert len(find_response["resources"]) == len(expected_resources), (
            "Unexpected number of find results"
        )
        assert set(find_response["resources"]) == set(expected_resources), "Unexpected find results"
    elif endpoint == "suggest":
        suggest_response = resp.json()
        resources = [paragraph["rid"] for paragraph in suggest_response["paragraphs"]["results"]]
        assert len(resources) == len(expected_resources), "Unexpected number of suggest results"
        assert set(resources) == set(expected_resources), "Unexpected suggest results"
    else:
        raise ValueError(f"Unknown method and/or search endpoint: {method} {endpoint}")


async def _create_public_resource(
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    kbid: str,
) -> str:
    """Helper to create a public resource (no security groups) with indexed paragraphs."""
    return await _create_resource(
        nucliadb_writer,
        nucliadb_ingest_grpc,
        kbid=kbid,
        title="Public resource",
        texts={"text1": "This is a public document about something"},
    )


@pytest.mark.parametrize(
    "search_endpoint",
    (
        ("GET", "find"),
        ("POST", "find"),
        ("GET", "search"),
        ("POST", "search"),
        ("GET", "suggest"),
        ("POST", "suggest"),
    ),
)
@pytest.mark.deploy_modes("standalone")
async def test_security_groups_enforce_hides_secured_resources_without_matching_groups(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox: str,
    resource_with_security,
    search_endpoint: tuple[str, str],
):
    """When security_groups_enforce is enabled at the KB level, resources with
    security groups should only be returned when the request provides matching
    security groups. Resources without security groups (public) should always be returned.

    Without security param: only public resource returned.
    With matching security groups: both resources returned.
    With non-matching security groups: only public resource returned.
    """
    kbid = standalone_knowledgebox
    secured_rid = resource_with_security
    method, endpoint = search_endpoint

    # Enable enforce security groups at KB level
    resp = await nucliadb_writer_manager.patch(
        f"/kb/{kbid}",
        json={
            "enforce_security": True,
        },
    )
    assert resp.status_code == 200, resp.text

    # Create a public resource (no security groups)
    public_rid = await _create_public_resource(nucliadb_writer, nucliadb_ingest_grpc, kbid)

    # Querying without security should only return the public resource
    await _test_search_request_with_security(
        method,
        endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[public_rid],
    )

    # Querying with matching security groups should return both resources
    for access_groups in (
        [PLATFORM_GROUP],
        [DEVELOPERS_GROUP],
        [PLATFORM_GROUP, DEVELOPERS_GROUP],
    ):
        await _test_search_request_with_security(
            method,
            endpoint,
            nucliadb_reader,
            kbid,
            query="resource",
            security_groups=access_groups,
            expected_resources=[secured_rid, public_rid],
        )

    # Querying with non-matching security groups should only return the public resource
    await _test_search_request_with_security(
        method,
        endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=["unknown-group"],
        expected_resources=[public_rid],
    )


@pytest.mark.parametrize(
    "search_endpoint",
    (
        ("GET", "find"),
        ("POST", "find"),
        ("GET", "search"),
        ("POST", "search"),
        ("GET", "suggest"),
        ("POST", "suggest"),
    ),
)
@pytest.mark.deploy_modes("standalone")
async def test_security_groups_enforce_disabled_returns_all_resources(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
    search_endpoint: tuple[str, str],
):
    """When security_groups_enforce is NOT enabled (default), requests without
    security groups should return all resources, including those with security groups.
    This is the current/legacy behavior.
    """
    kbid = standalone_knowledgebox
    secured_rid = resource_with_security
    method, endpoint = search_endpoint

    # Without security_groups_enforce, querying without security should return the resource
    await _test_search_request_with_security(
        method,
        endpoint,
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[secured_rid],
    )


@pytest.mark.deploy_modes("standalone")
async def test_security_groups_enforce_toggle(
    nucliadb_reader: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
):
    """Test that toggling security_groups_enforce on and off changes the behavior."""
    kbid = standalone_knowledgebox
    secured_rid = resource_with_security

    # By default, enforce is off: querying without security returns everything
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[secured_rid],
    )

    # Enable enforce
    resp = await nucliadb_writer_manager.patch(
        f"/kb/{kbid}",
        json={"enforce_security": True},
    )
    assert resp.status_code == 200, resp.text

    # Now querying without security should NOT return the secured resource
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[],
    )

    # Querying with matching security groups should return it
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=[PLATFORM_GROUP],
        expected_resources=[secured_rid],
    )

    # Disable enforce
    resp = await nucliadb_writer_manager.patch(
        f"/kb/{kbid}",
        json={"enforce_security": False},
    )
    assert resp.status_code == 200, resp.text

    # Back to default behavior: querying without security returns everything again
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[secured_rid],
    )


@pytest.mark.deploy_modes("standalone")
async def test_security_groups_enforce_making_resource_public(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
    standalone_knowledgebox: str,
    resource_with_security,
):
    """When security_groups_enforce is enabled, removing all security groups from a
    resource (making it public) should make it visible to all requests again.
    """
    kbid = standalone_knowledgebox
    rid = resource_with_security

    # Enable enforce
    resp = await nucliadb_writer_manager.patch(
        f"/kb/{kbid}",
        json={"enforce_security": True},
    )
    assert resp.status_code == 200, resp.text

    # Querying without security should NOT return the resource
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[],
    )

    # Make the resource public by removing security groups
    resp = await nucliadb_writer.patch(
        f"/kb/{kbid}/resource/{rid}",
        json={
            "security": {
                "access_groups": [],
            },
        },
    )
    assert resp.status_code == 200, resp.text

    # Wait for the tantivy index to be updated
    await asyncio.sleep(1)

    # Now querying without security should return the resource
    await _test_search_request_with_security(
        "POST",
        "find",
        nucliadb_reader,
        kbid,
        query="resource",
        security_groups=None,
        expected_resources=[rid],
    )
