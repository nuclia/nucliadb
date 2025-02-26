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
from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient

from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message


@pytest.mark.deploy_modes("standalone")
async def test_catalog_pagination(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    n_resources = 35
    for i in range(n_resources):
        resp = await nucliadb_writer.post(
            f"/kb/{standalone_knowledgebox}/resources",
            json={
                "title": f"Resource {i}",
                "texts": {
                    "text": {
                        "body": f"Text for resource {i}",
                    }
                },
            },
        )
        assert resp.status_code == 201

    # Give some time for the resources to be refreshed in the index
    await asyncio.sleep(1)

    resource_uuids = []
    creation_dates = []
    page_size = 10
    page_number = 0
    while True:
        resp = await nucliadb_reader.get(
            f"/kb/{standalone_knowledgebox}/catalog",
            params={
                "page_number": page_number,
                "page_size": page_size,
            },
        )
        assert resp.status_code == 200, resp.text
        body = resp.json()
        assert len(body["resources"]) <= page_size
        assert body["fulltext"]["page_number"] == page_number
        for resource_id, resource_data in body["resources"].items():
            resource_created_date = datetime.fromisoformat(resource_data["created"]).timestamp()
            if resource_id in resource_uuids:
                assert False, f"Resource {resource_id} already seen"
            resource_uuids.append(resource_id)
            creation_dates.append(resource_created_date)
        if not body["fulltext"]["next_page"]:
            break
        page_number += 1
    assert len(resource_uuids) == n_resources


@pytest.mark.deploy_modes("standalone")
async def test_catalog_date_range_filtering(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    standalone_knowledgebox,
):
    now = datetime.now()
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": f"Resource",
            "texts": {
                "text": {
                    "body": f"Text for resource",
                }
            },
        },
    )
    assert resp.status_code == 201

    one_hour_ago = now - timedelta(hours=1)
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog",
        params={
            "range_creation_start": one_hour_ago.isoformat(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "range_creation_end": one_hour_ago.isoformat(),
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0


@pytest.mark.deploy_modes("standalone")
async def test_catalog_status_faceted(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    valid_status = ["PROCESSED", "PENDING", "ERROR"]
    resources = {}

    for status_name, status_value in rpb.Metadata.Status.items():
        if status_name not in valid_status:
            continue
        bm = broker_resource(standalone_knowledgebox)
        bm.basic.metadata.status = status_value
        await inject_message(nucliadb_ingest_grpc, bm)
        resources[status_name] = bm.uuid

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?faceted=/metadata.status",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 3
    facets = body["fulltext"]["facets"]["/metadata.status"]
    assert len(facets) == 3
    for facet, count in facets.items():
        assert facet.split("/")[-1] in valid_status
        assert count == 1

    for status in valid_status:
        resource = resources[status]
        resp = await nucliadb_reader.post(
            f"/kb/{standalone_knowledgebox}/catalog",
            json={"filter_expression": {"resource": {"prop": "status", "status": status}}},
        )
        assert resp.status_code == 200
        assert set(resp.json()["resources"].keys()) == {resource}


@pytest.mark.deploy_modes("standalone")
async def test_catalog_faceted_labels(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    # 4 resources:
    # 1 with /l/labelset0/label0
    # 2 with /l/labelset0/label1
    # 1 with /l/labelset1/label0
    for label in range(2):
        for count in range(label + 1):
            bm = broker_resource(standalone_knowledgebox)
            c = rpb.Classification()
            c.labelset = f"labelset0"
            c.label = f"label{label}"
            bm.basic.usermetadata.classifications.append(c)
            await inject_message(nucliadb_ingest_grpc, bm)

    bm = broker_resource(standalone_knowledgebox)
    c = rpb.Classification()
    c.labelset = "labelset1"
    c.label = "label0"
    bm.basic.usermetadata.classifications.append(c)
    await inject_message(nucliadb_ingest_grpc, bm)

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?faceted=/classification.labels/labelset0",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["fulltext"]["facets"] == {
        "/classification.labels/labelset0": {
            "/classification.labels/labelset0/label0": 1,
            "/classification.labels/labelset0/label1": 2,
        }
    }

    # This is used by the check missing labels button in dashboard
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?faceted=/classification.labels",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["fulltext"]["facets"] == {
        "/classification.labels": {
            "/classification.labels/labelset0": 3,
            "/classification.labels/labelset1": 1,
        }
    }


@pytest.mark.deploy_modes("standalone")
async def test_catalog_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    valid_status = ["PROCESSED", "PENDING", "ERROR"]

    for status_name, status_value in rpb.Metadata.Status.items():
        if status_name not in valid_status:
            continue
        bm = broker_resource(standalone_knowledgebox)
        bm.basic.metadata.status = status_value
        await inject_message(nucliadb_ingest_grpc, bm)

    # No filters
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 3

    # Simple filter
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?filters=/metadata.status/PENDING",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1
    assert list(body["resources"].values())[0]["metadata"]["status"] == "PENDING"

    # AND filter
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={"filters": [{"all": ["/metadata.status/PENDING", "/metadata.status/ERROR"]}]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # OR filter
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={"filters": [{"any": ["/metadata.status/PENDING", "/metadata.status/ERROR"]}]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 2
    for resource in body["resources"].values():
        assert resource["metadata"]["status"] in ["PENDING", "ERROR"]


@pytest.mark.deploy_modes("standalone")
async def test_catalog_post(
    nucliadb_reader: AsyncClient,
    standalone_knowledgebox,
):
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "query": "",
            "filters": [
                {"any": ["/foo", "/bar"]},
            ],
            "with_status": "PROCESSED",
            "sort": {
                "field": "created",
            },
        },
    )
    assert resp.status_code == 200


@pytest.mark.deploy_modes("standalone")
async def test_catalog_by_path_filter(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    paths = ["/foo", "foo/bar", "foo/bar/1", "foo/bar/2", "foo/bar/3", "foo/bar/4"]

    for path in paths:
        resp = await nucliadb_writer.post(
            f"/kb/{standalone_knowledgebox}/resources",
            json={
                "title": f"My resource: {path}",
                "summary": "Some summary",
                "origin": {
                    "path": path,
                },
            },
        )
        assert resp.status_code == 201

    # Get the list of all
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/catalog?filters=/origin.path/foo")
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == len(paths)

    # Get the list of under foo/bar
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?filters=/origin.path/foo/bar"
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == len(paths) - 1

    # Get the list of under foo/bar/4
    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/catalog?filters=/origin.path/foo/bar/4"
    )
    assert resp.status_code == 200
    assert len(resp.json()["resources"]) == 1


@pytest.mark.deploy_modes("standalone")
async def test_catalog_filter_expression(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": f"My resource 1",
            "summary": "Some summary",
            "origin": {"path": "/folder/file1", "tags": ["wadus", "wadus1"]},
        },
    )
    assert resp.status_code == 201
    resource1 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": f"My resource 2",
            "summary": "Some summary",
            "origin": {"path": "/folder/file2", "tags": ["wadus", "wadus2"]},
        },
    )
    assert resp.status_code == 201
    resource2 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "title": f"My resource 3",
            "summary": "Some summary",
            "origin": {"collaborators": ["Anna", "Peter"], "source_id": "internet"},
        },
    )
    assert resp.status_code == 201
    resource3 = resp.json()["uuid"]

    # Mixing with old filters not allowed
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filters": ["/l/abc"],
            "filter_expression": {"resource": {"prop": "resource", "id": resource3}},
        },
    )
    assert resp.status_code == 412

    # By prefix
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {"resource": {"prop": "origin_path", "prefix": "folder"}},
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource1, resource2}

    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {"resource": {"not": {"prop": "origin_path"}}},
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource3}

    # And
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {
                "resource": {
                    "and": [
                        {"prop": "origin_tag", "tag": "wadus"},
                        {"prop": "origin_path", "prefix": "folder/file2"},
                    ]
                }
            },
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource2}

    # Or
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {
                "resource": {
                    "or": [
                        {"prop": "origin_tag", "tag": "wadus1"},
                        {"prop": "origin_collaborator", "collaborator": "Peter"},
                    ]
                }
            },
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource1, resource3}

    # Not
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {"resource": {"not": {"prop": "origin_tag", "tag": "wadus"}}},
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource3}

    # Combining everything
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/catalog",
        json={
            "filter_expression": {
                "resource": {
                    "or": [
                        {
                            "and": [
                                {"prop": "origin_tag", "tag": "wadus"},
                                {"prop": "origin_path", "prefix": "folder/file1"},
                                {"not": {"prop": "modified", "until": "2019-01-01T11:00:00"}},
                            ]
                        },
                        {"prop": "origin_source", "id": "internet"},
                    ]
                }
            }
        },
    )
    assert resp.status_code == 200
    assert set(resp.json()["resources"].keys()) == {resource1, resource3}
