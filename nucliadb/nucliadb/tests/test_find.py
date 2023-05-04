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

import asyncio

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_find_with_label_changes(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    await asyncio.sleep(1)

    # should get 1 result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # assert we get no results with label filter
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/l/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0

    # add new label
    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            # "title": "My new title",
            "usermetadata": {
                "classifications": [
                    {
                        "labelset": "labels",
                        "label": "label1",
                        "cancelled_by_user": False,
                    }
                ],
                "relations": [],
            }
        },
        headers={"X-SYNCHRONOUS": "True"},
        timeout=None,
    )
    assert resp.status_code == 200
    await asyncio.sleep(1)

    # we should get 1 result now with updated label
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "filters": ["/l/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1


@pytest.mark.asyncio
async def test_find_does_not_support_fulltext_search(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/find?query=title&features=document&features=paragraph",
    )
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["msg"] == "fulltext search not supported"

    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={"query": "title", "features": ["document", "paragraph"]},
    )
    assert resp.status_code == 422
    assert resp.json()["detail"][0]["msg"] == "fulltext search not supported"
