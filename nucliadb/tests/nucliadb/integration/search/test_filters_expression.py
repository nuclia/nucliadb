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
import pytest


@pytest.mark.asyncio
async def test_filtering_expression(nucliadb_reader, nucliadb_writer, knowledgebox):
    kbid = knowledgebox

    slug_to_uuid = {}
    # Create 3 resources in different folders
    for title, slug, path, tag in [
        ("Resource1", "resource1", "folder1", "news"),
        ("Resource2", "resource2", "folder2", "poetry"),
        ("Resource3", "resource3", "folder3", "scientific"),
    ]:
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": title,
                "slug": slug,
                "origin": {
                    "path": path,
                    "tags": [tag],
                },
            },
        )
        assert resp.status_code == 201
        slug_to_uuid[slug] = resp.json()["uuid"]

    for filters, expected_slugs in [
        # all: [a, b] == (a && b)
        ([{"all": ["/origin.path/folder1", "/origin.path/folder2"]}], []),
        ([{"all": ["/origin.path/folder1", "/origin.tags/news"]}], ["resource1"]),
        # any: [a, b] == (a || b)
        (
            [{"any": ["/origin.path/folder1", "/origin.path/folder2"]}],
            ["resource1", "resource2"],
        ),
        ([{"any": ["/origin.path/folder1", "/inexisting"]}], ["resource1"]),
        # none: [a, b] == !(a || b)
        ([{"none": ["/origin.path/folder1"]}], ["resource2", "resource3"]),
        ([{"none": ["/origin.path/folder1", "/origin.path/folder2"]}], ["resource3"]),
        # not_all: [a, b] == !(a && b)
        (
            [{"not_all": ["/origin.path/folder1", "/origin.path/folder2"]}],
            ["resource1", "resource2", "resource3"],
        ),
        (
            [{"not_all": ["/origin.path/folder1", "/origin.tags/news"]}],
            ["resource2", "resource3"],
        ),
        # combined expressions
        (
            [
                {
                    "any": [
                        "/origin.path/folder1",
                        "/origin.path/folder2",
                        "/origin.path/folder3",
                    ]
                },
                {"none": ["/origin.tags/news"]},
            ],
            ["resource2", "resource3"],
        ),
    ]:
        resp = await nucliadb_reader.post(
            f"/kb/{kbid}/find",
            json={"query": "", "filters": filters},
        )
        assert resp.status_code == 200
        body = resp.json()
        expected_uuids = {slug_to_uuid[slug] for slug in expected_slugs}
        found_uuids = set(body["resources"].keys())
        assert found_uuids == expected_uuids


@pytest.mark.asyncio
async def test_filtering_expression_validation(nucliadb_reader, nucliadb_writer):
    # Make sure we only allow one operator per filter
    resp = await nucliadb_reader.post(
        f"/kb/foobar/find",
        json={
            "query": "",
            "filters": [{"all": ["/origin.path/folder1"], "any": ["/origin.path/folder2"]}],
        },
    )
    assert resp.status_code == 422
    assert "Only one of 'all', 'any', 'none' or 'not_all' can be set" in resp.json()["detail"][-1]["msg"]

    # Empty lists of filter operators are not allowed
    resp = await nucliadb_reader.post(
        f"/kb/foobar/find",
        json={"query": "", "filters": [{"all": []}]},
    )
    assert resp.status_code == 422
    assert "List should have at least 1 item" in resp.json()["detail"][-1]["msg"]

    # But we ignore None values
    resp = await nucliadb_reader.post(
        f"/kb/foobar/find",
        json={"query": "", "filters": [{"all": ["/origin.path/folder1"], "any": None}]},
    )
    assert resp.status_code != 422
