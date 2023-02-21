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


@pytest.fixture(scope="function")
async def ten_dummy_resources_kb(
    nucliadb_manager: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    N_RESOURCES = 10

    payloads = [
        {
            "slug": f"dummy-resource-{i}",
            "title": f"Dummy resource {i}",
            "summary": f"Dummy resource {i} summary",
        }
        for i in range(N_RESOURCES)
    ]

    resp = await nucliadb_manager.post("/kbs", json={"slug": "ten-dummy-resources"})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    for payload in payloads:
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            headers={"X-Synchronous": "true"},
            json=payload,
        )
        assert resp.status_code == 201

        await asyncio.sleep(1)

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resources",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == N_RESOURCES

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200


@pytest.fixture(scope="function")
async def ten_quick_dummy_resources_kb(
    nucliadb_manager: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
):
    N_RESOURCES = 10

    payloads = [
        {
            "slug": f"dummy-resource-{i}",
            "title": f"Dummy resource {i}",
            "summary": f"Dummy resource {i} summary",
        }
        for i in range(N_RESOURCES)
    ]

    resp = await nucliadb_manager.post("/kbs", json={"slug": "ten-dummy-resources"})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    for payload in payloads:
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            headers={"X-Synchronous": "true"},
            json=payload,
        )
        assert resp.status_code == 201

    resp = await nucliadb_reader.get(
        f"/kb/{kbid}/resources",
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == N_RESOURCES

    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    assert resp.status_code == 200
