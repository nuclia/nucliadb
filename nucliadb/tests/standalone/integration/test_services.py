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
import random

import pytest

from nucliadb.common.maindb.tikv import TiKVDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search.api.v1.router import KB_PREFIX


@pytest.mark.asyncio
async def test_entities_service(
    nucliadb_reader,
    nucliadb_writer,
    knowledgebox_one,
    entities_manager_mock,
) -> None:
    entitygroup = {
        "group": "group1",
        "title": "Kitchen",
        "custom": True,
        "entities": {
            "cupboard": {"value": "Cupboard"},
            "fork": {"value": "Fork"},
            "fridge": {"value": "Fridge"},
            "knife": {"value": "Knife"},
            "sink": {"value": "Sink"},
            "spoon": {"value": "Spoon"},
        },
    }
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroups", json=entitygroup
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroups")
    assert resp.status_code == 200
    groups = resp.json()["groups"]
    assert len(groups) == 1
    assert groups["group1"]["custom"] is True

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroup/group1")
    assert resp.status_code == 200
    assert resp.json()["custom"] is True

    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroup/group1")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/entitiesgroups")
    assert resp.status_code == 200
    assert len(resp.json()["groups"]) == 0


@pytest.mark.asyncio
async def test_labelsets_service(nucliadb_reader, nucliadb_writer, knowledgebox_one) -> None:
    payload = {
        "title": "labelset1",
        "labels": [{"title": "Label 1", "related": "related 1", "text": "My Text"}],
    }
    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1", json=payload
    )
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelsets")
    assert len(resp.json()["labelsets"]) == 1

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1")
    assert resp.status_code == 200

    resp = await nucliadb_writer.delete(f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox_one}/labelsets")
    assert len(resp.json()["labelsets"]) == 0


@pytest.mark.asyncio
async def test_labelsets_service_handles_conflicts(
    nucliadb_reader, nucliadb_writer, knowledgebox_one
) -> None:
    driver = get_driver()
    if not isinstance(driver, TiKVDriver):
        pytest.skip("Only tikv driver will raise conflicts")

    conflict_reproduced = asyncio.Event()
    conflict_reproduced.clear()

    async def post_labelset():
        labels = [{"title": f"Label {i}"} for i in range(random.randint(1, 3))]
        payload = {
            "title": "labelset1",
            "labels": labels,
        }
        resp = await nucliadb_writer.post(
            f"/{KB_PREFIX}/{knowledgebox_one}/labelset/labelset1", json=payload
        )
        assert resp.status_code in (200, 409)
        if resp.status_code == 409:
            conflict_reproduced.set()

    tasks = [post_labelset() for _ in range(5)]
    await asyncio.gather(*tasks)
    assert conflict_reproduced.is_set(), "Conflict was not reproduced"


async def test_notifications_service(nucliadb_reader):
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/foobar/notifications")
    assert resp.status_code == 404
