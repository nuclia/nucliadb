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
from unittest import mock

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_last_seqid_is_stored_in_resource(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "texts": {
                "textfield1": {"body": "Some text", "format": "PLAIN"},
            }
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]
    seqid = data["seqid"]

    # last_seqid should be returned on a resource get
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert seqid == resp.json()["last_seqid"]

    # last_seqid should be returned when listing resources
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resources")
    resource_list = resp.json()["resources"]
    assert len(resource_list) > 0
    for resource in resource_list:
        assert "last_seqid" in resource


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_crud(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "slug": "mykb",
            "title": "My KB",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "My KB"

    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
        headers={"X-Synchronous": "true"},
        json={
            "title": "My updated KB",
        },
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["title"] == "My updated KB"

    resp = await nucliadb_writer.delete(
        f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 204

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 404


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_list_resources(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    """
    - Create 20 resources
    - Then check that scrolling the whole resource list returns
      the created resources without errors.
    """
    rids = set()
    for _ in range(20):
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
            headers={"X-Synchronous": "true"},
            json={
                "title": "My resource",
            },
        )
        assert resp.status_code == 201
        rids.add(resp.json()["uuid"])

    got_rids = set()
    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=0"
    )
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    resp = await nucliadb_reader.get(
        f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=1"
    )
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    assert got_rids == rids


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_get_resource_field(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    slug = "my-resource"
    field = "text-field"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "slug": slug,
            "title": "My Resource",
            "texts": {field: {"body": "test1", "format": "PLAIN"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}/text/{field}")
    assert resp.status_code == 200
    body_by_slug = resp.json()

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/slug/{slug}/text/{field}")
    assert resp.status_code == 200
    body_by_rid = resp.json()

    assert body_by_slug == body_by_rid


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_creation_slug_conflicts(
    nucliadb_writer: AsyncClient,
    knowledgebox,
    philosophy_books_kb,
):
    """
    Test that creating two resources with the same slug raises a conflict error
    """
    slug = "myresource"
    resources_path = f"/{KB_PREFIX}/{{knowledgebox}}/{RESOURCES_PREFIX}"
    resp = await nucliadb_writer.post(
        resources_path.format(knowledgebox=knowledgebox),
        headers={"X-Synchronous": "true"},
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        resources_path.format(knowledgebox=knowledgebox),
        headers={"X-Synchronous": "true"},
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 409

    # Creating it in another KB should not raise conflict error
    resp = await nucliadb_writer.post(
        resources_path.format(knowledgebox=philosophy_books_kb),
        headers={"X-Synchronous": "true"},
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 201


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_title_is_set_automatically_if_not_provided(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "texts": {"text-field": {"body": "test1", "format": "PLAIN"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["title"] == rid


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("update_by", ["slug", "uuid"])
@pytest.mark.parametrize("x_synchronous", [True, False])
async def test_resource_slug_modification(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
    update_by,
    x_synchronous,
):
    old_slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "title": "My Resource",
            "slug": old_slug,
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    await check_resource(nucliadb_reader, knowledgebox, rid, old_slug)

    # Update the slug
    new_slug = "my-resource-2"
    if update_by == "slug":
        path = f"/{KB_PREFIX}/{knowledgebox}/slug/{old_slug}"
    else:
        path = f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}"
    resp = await nucliadb_writer.patch(
        path,
        headers={"X-Synchronous": str(x_synchronous).lower()},
        json={
            "slug": new_slug,
            "title": "New title",
        },
        timeout=None,
    )
    assert resp.status_code == 200

    await check_resource(
        nucliadb_reader, knowledgebox, rid, new_slug, title="New title"
    )


async def check_resource(nucliadb_reader, kbid, rid, slug, **body_checks):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}")
    assert resp.status_code == 200
    assert resp.json()["slug"] == slug

    resp = await nucliadb_reader.get(f"/kb/{kbid}/slug/{slug}")
    assert resp.status_code == 200
    body = resp.json()
    assert body["id"] == rid
    for key, value in body_checks.items():
        assert body[key] == value


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_slug_modification_rollbacks(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
):
    old_slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-Synchronous": "true"},
        json={
            "title": "Old title",
            "slug": old_slug,
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    await check_resource(nucliadb_reader, knowledgebox, rid, old_slug)

    # Mock an error in the sending to process
    with mock.patch(
        "nucliadb.writer.api.v1.resource.maybe_send_to_process",
        side_effect=HTTPException(status_code=506),
    ):
        resp = await nucliadb_writer.patch(
            f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}",
            headers={"X-Synchronous": "true"},
            json={
                "slug": "my-resource-2",
                "title": "New title",
            },
        )
        assert resp.status_code == 506

    # Check that slug and title were not updated
    await check_resource(
        nucliadb_reader, knowledgebox, rid, old_slug, title="Old title"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_slug_modification_handles_conflicts(
    nucliadb_writer,
    knowledgebox,
):
    rids = []
    slugs = []
    for i in range(2):
        slug = f"my-resource-{i}"
        slugs.append(slug)
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
            headers={"X-Synchronous": "true"},
            json={
                "title": "My Resource",
                "slug": slug,
            },
        )
        assert resp.status_code == 201
        rid = resp.json()["uuid"]
        rids.append(rid)

    # Check that conflicts on slug are detected
    path = f"/{KB_PREFIX}/{knowledgebox}/resource/{rids[0]}"
    resp = await nucliadb_writer.patch(
        path,
        headers={"X-Synchronous": "true"},
        json={
            "slug": slugs[1],
        },
    )
    assert resp.status_code == 409


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_resource_slug_modification_handles_unknown_resources(
    nucliadb_writer,
    knowledgebox,
):
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox}/resource/foobar",
        headers={"X-Synchronous": "true"},
        json={
            "slug": "foo",
        },
    )
    assert resp.status_code == 404
