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
from unittest import mock

import pytest
from fastapi import HTTPException
from httpx import AsyncClient

from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX


async def test_resource_crud(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
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
    )
    assert resp.status_code == 204

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 404


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
            json={
                "title": "My resource",
            },
        )
        assert resp.status_code == 201
        rids.add(resp.json()["uuid"])

    got_rids = set()
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=0")
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resources?size=10&page=1")
    assert resp.status_code == 200
    for r in resp.json()["resources"]:
        got_rids.add(r["id"])

    assert got_rids == rids


async def test_get_resource_field(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    slug = "my-resource"
    field = "text-field"

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
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
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 201

    resp = await nucliadb_writer.post(
        resources_path.format(knowledgebox=knowledgebox),
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 409

    # Creating it in another KB should not raise conflict error
    resp = await nucliadb_writer.post(
        resources_path.format(knowledgebox=philosophy_books_kb),
        json={
            "slug": slug,
        },
    )
    assert resp.status_code == 201


async def test_title_is_set_automatically_if_not_provided(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
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


@pytest.mark.parametrize("update_by", ["slug", "uuid"])
async def test_resource_slug_modification(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
    update_by,
):
    old_slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
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
        json={
            "slug": new_slug,
            "title": "New title",
        },
    )
    assert resp.status_code == 200

    await check_resource(nucliadb_reader, knowledgebox, rid, new_slug, title="New title")


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


async def test_resource_slug_modification_rollbacks(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox,
):
    old_slug = "my-resource"
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
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
            json={
                "slug": "my-resource-2",
                "title": "New title",
            },
        )
        assert resp.status_code == 506

    # Check that slug and title were not updated
    await check_resource(nucliadb_reader, knowledgebox, rid, old_slug, title="New title")


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
        json={
            "slug": slugs[1],
        },
    )
    assert resp.status_code == 409


async def test_resource_slug_modification_handles_unknown_resources(
    nucliadb_writer,
    knowledgebox,
):
    resp = await nucliadb_writer.patch(
        f"/{KB_PREFIX}/{knowledgebox}/resource/foobar",
        json={
            "slug": "foo",
        },
    )
    assert resp.status_code == 404


async def test_parallel_dup_resource_creation_raises_conflicts(
    nucliadb_writer,
    knowledgebox,
):
    driver = get_driver()
    if not isinstance(driver, PGDriver):
        pytest.skip("local driver is not totally safe in terms of slug uniqueness")

    slug = "foobar-unique"

    async def create_resource(kbid: str):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": "My resource",
                "slug": slug,
            },
        )
        return resp.status_code

    # Create 5 requests that attempt to create the same resource with the same slug simultaneously
    tasks = []
    for _ in range(5):
        tasks.append(asyncio.create_task(create_resource(knowledgebox)))
    status_codes = await asyncio.gather(*tasks)

    # Check that only one succeeded
    assert status_codes.count(201) == 1
    assert status_codes.count(409) == 4
