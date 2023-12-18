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
from unittest.mock import patch

import pytest
from httpx import AsyncClient
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
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
        json={"query": "title", "filters": ["/classification.labels/labels/label1"]},
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
        json={"query": "title", "filters": ["/classification.labels/labels/label1"]},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
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


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_find_resource_filters(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
        headers={"X-SYNCHRONOUS": "True"},
    )
    assert resp.status_code == 201
    rid1 = resp.json()["uuid"]

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
        headers={"X-SYNCHRONOUS": "True"},
    )
    assert resp.status_code == 201
    rid2 = resp.json()["uuid"]

    # Should get 2 result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 2

    # Check that resource filtering works
    for rid in [rid1, rid2]:
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/find",
            json={
                "query": "title",
                "resource_filters": [rid],
            },
        )
        assert resp.status_code == 200
        body = resp.json()
        assert len(body["resources"]) == 1
        assert rid in body["resources"]


@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_find_min_score(
    nucliadb_reader: AsyncClient,
    knowledgebox,
):
    # When not specifying the min score on the request, it should default to 0.7
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find", json={"query": "dummy"}
    )
    assert resp.status_code == 200
    assert resp.json()["min_score"] == 0.7

    # If we specify a min score, it should be used
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find", json={"query": "dummy", "min_score": 0.5}
    )
    assert resp.status_code == 200
    assert resp.json()["min_score"] == 0.5


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_story_7286(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
    caplog,
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

    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            "fieldmetadata": [
                {
                    "field": {
                        "field": "text1",
                        "field_type": "text",
                    },
                    "paragraphs": [
                        {
                            "key": f"{rid}/t/text1/0-7",
                            "classifications": [{"labelset": "ls1", "label": "label"}],
                        }
                    ],
                }
            ]
        },
    )
    assert resp.status_code == 200

    with patch("nucliadb.search.search.find_merge.serialize", return_value=None):
        # should get no result (because serialize returns None, as the resource is not found in the DB)
        resp = await nucliadb_reader.post(
            f"/kb/{knowledgebox}/find",
            json={
                "query": "title",
                "features": ["paragraph", "vector", "relations"],
                "shards": [],
                "highlight": True,
                "autofilter": False,
                "page_number": 0,
                "show": ["basic", "values", "origin"],
                "filters": [],
            },
        )
        assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 0
    assert caplog.record_tuples[0][2] == f"Resource {rid} not found in {knowledgebox}"


@pytest.mark.asyncio
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_find_marks_fuzzy_results(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
        },
    )
    assert resp.status_code == 201

    # Should get only one non-fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Title",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=False, n_expected=1)

    # Should get only one fuzzy result
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "totle",
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=1)

    # Should not get any result if exact match term queried
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": '"totle"',
        },
    )
    assert resp.status_code == 200
    body = resp.json()
    check_fuzzy_paragraphs(body, fuzzy_result=True, n_expected=0)


def check_fuzzy_paragraphs(find_response, *, fuzzy_result: bool, n_expected: int):
    found = 0
    for resource in find_response["resources"].values():
        for field in resource["fields"].values():
            for paragraph in field["paragraphs"].values():
                assert paragraph["fuzzy_result"] is fuzzy_result
                found += 1
    assert found == n_expected
