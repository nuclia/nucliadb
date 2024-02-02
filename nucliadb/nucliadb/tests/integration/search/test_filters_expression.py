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
@pytest.mark.parametrize("knowledgebox", ["STABLE", "EXPERIMENTAL"], indirect=True)
async def test_filtering_expression(nucliadb_reader, nucliadb_writer, knowledgebox):
    kbid = knowledgebox

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
            headers={"x-synchronous": "true"},
        )
        assert resp.status_code == 201

    errored = []
    for filters, expected_slugs in [
        # all: [a, b] == (a && b)
        ([{"all": ["/origin.path/folder1", "/origin.path/folder2"]}], []),
        ([{"all": ["/origin.path/folder1", "/origin.tags/news"]}], ["resource1"]),
        (
            [
                {
                    "all": [
                        "/origin.path/folder1",
                        {"or": ["/origin.tags/news", "inexisting"]},
                    ]
                }
            ],
            ["resource1"],
        ),
        # any: [a, b] == (a || b)
        (
            [{"any": ["/origin.path/folder1", "/origin.path/folder2"]}],
            ["resource1", "resource2"],
        ),
        ([{"any": ["/origin.path/folder1", "/inexisting"]}, ["resource1"]]),
        # none: [a, b] == !(a || b)
        ([{"none": ["/origin.path/folder1"]}], ["resource2", "resource3"]),
        ({"none": ["/origin.path/folder1", "/origin.path/folder2"]}, ["resource3"]),
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
            f"/kb/{kbid}/find", json={"query": "", "filters": filters}
        )
        assert resp.status_code == 200
        body = resp.json()
        try:
            assert set(body["resources"].keys()) == set(expected_slugs)
        except AssertionError:
            errored.append((filters, expected_slugs, body))
    assert errored == []
