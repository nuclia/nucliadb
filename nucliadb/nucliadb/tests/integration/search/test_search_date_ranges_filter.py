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
from datetime import datetime, timedelta

import pytest
from httpx import AsyncClient

NOW = datetime.now()
ORIGIN_CREATION = datetime(2021, 1, 1)
ORIGIN_MODIFICATION = datetime(2022, 1, 1)


def a_week_after(date):
    return date + timedelta(days=7)


def a_week_before(date):
    return date - timedelta(days=7)


@pytest.mark.parametrize(
    "creation_start,creation_end,modification_start,modification_end,found",
    [
        # Inside queried date range
        (a_week_before(NOW), None, None, None, True),
        (None, a_week_after(NOW), None, None, True),
        (None, None, a_week_before(NOW), None, True),
        (None, None, None, a_week_after(NOW), True),
        (
            a_week_before(NOW),
            a_week_after(NOW),
            a_week_before(NOW),
            a_week_after(NOW),
            True,
        ),
        # Outside queried date range
        (a_week_after(NOW), None, None, None, False),
        (None, a_week_before(NOW), None, None, False),
        (None, None, a_week_after(NOW), None, False),
        (None, None, None, a_week_before(NOW), False),
        (a_week_before(NOW), None, a_week_after(NOW), None, False),
    ],
)
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_bm25_search_with_date_range_filters_nucliadb_dates(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
    creation_start,
    creation_end,
    modification_start,
    modification_end,
    found,
):
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My resource",
        },
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 201

    await _test_find_date_ranges(
        nucliadb_reader,
        kbid,
        creation_start,
        creation_end,
        modification_start,
        modification_end,
        found,
    )


@pytest.mark.parametrize(
    "creation_start,creation_end,modification_start,modification_end,found",
    [
        # Inside queried date range
        (ORIGIN_CREATION, None, None, None, True),
        (None, ORIGIN_CREATION, None, None, True),
        (None, None, ORIGIN_MODIFICATION, None, True),
        (None, None, None, ORIGIN_MODIFICATION, True),
        (
            a_week_before(ORIGIN_CREATION),
            a_week_after(ORIGIN_CREATION),
            a_week_before(ORIGIN_MODIFICATION),
            a_week_after(ORIGIN_MODIFICATION),
            True,
        ),
        # Outside queried date range
        (a_week_after(ORIGIN_CREATION), None, None, None, False),
        (None, a_week_before(ORIGIN_CREATION), None, None, False),
        (None, None, a_week_after(ORIGIN_MODIFICATION), None, False),
        (None, None, None, a_week_before(ORIGIN_MODIFICATION), False),
        (ORIGIN_CREATION, None, None, a_week_before(ORIGIN_MODIFICATION), False),
    ],
)
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
async def test_bm25_search_with_date_range_filters_origin_dates(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
    creation_start,
    creation_end,
    modification_start,
    modification_end,
    found,
):
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "My resource",
            "origin": {
                "created": ORIGIN_CREATION.isoformat(),
                "modified": ORIGIN_MODIFICATION.isoformat(),
            },
        },
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 201

    await _test_find_date_ranges(
        nucliadb_reader,
        kbid,
        creation_start,
        creation_end,
        modification_start,
        modification_end,
        found,
    )


async def _test_find_date_ranges(
    nucliadb_reader,
    kbid,
    creation_start,
    creation_end,
    modification_start,
    modification_end,
    found,
):
    payload = {
        "query": "resource",
        "features": ["paragraph"],
    }
    if creation_start:
        payload["range_creation_start"] = creation_start.isoformat()
    if creation_end:
        payload["range_creation_end"] = creation_end.isoformat()
    if modification_start:
        payload["range_modification_start"] = modification_start.isoformat()
    if modification_end:
        payload["range_modification_end"] = modification_end.isoformat()

    resp = await nucliadb_reader.post(f"/kb/{kbid}/find", json=payload)
    assert resp.status_code == 200
    body = resp.json()
    paragraphs = parse_paragraphs(body)
    if found:
        assert len(paragraphs) == 1
        assert "My resource" in paragraphs
    else:
        assert len(paragraphs) == 0


def parse_paragraphs(body):
    return [
        par["text"]
        for res in body.get("resources", {}).values()
        for field in res.get("fields", {}).values()
        for par in field.get("paragraphs", {}).values()
    ]


"""
range_creation_start
range_creation_end
range_modification_start
range_modification_end
"""
