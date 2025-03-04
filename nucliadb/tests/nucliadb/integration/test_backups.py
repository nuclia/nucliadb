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
import uuid
from httpx import AsyncClient
from nucliadb.backups.create import backup_kb
from nucliadb.common.context import ApplicationContext
import pytest


async def create_kb(
    nucliadb_writer_manager: AsyncClient,
):
    slug = uuid.uuid4().hex
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")
    return kbid

@pytest.fixture(scope="function")
async def src_kb(
    nucliadb_writer: AsyncClient,
    nucliadb_writer_manager: AsyncClient,
):
    kbid = await create_kb(nucliadb_writer_manager)
    
    # Create 10 simple resources with a text field
    for i in range(10):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": "Test",
                "thumbnail": "foobar",
                "icon": "application/pdf",
                "slug": f"test-{i}",
                "texts": {
                    "text": {
                        "body": f"This is a test {i}",
                    }
                }
            },
        )
        assert resp.status_code == 201

    # Create an entity group with a few entities
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/entitiesgroups",
        json={
            "group": "foo",
            "entities": {
                "bar": {"value": "BAZ", "represents": ["lorem", "ipsum"]},
            },
            "title": "Foo title",
            "color": "red",
        },
    )
    assert resp.status_code == 200

    # Create a labelset with a few labels
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/labelset/foo",
        json={
            "title": "Foo title",
            "color": "red",
            "multiple": True,
            "kind": ["RESOURCES"],
            "labels": [{"title": "Foo title", "text": "Foo text"}],
        },
    )
    assert resp.status_code == 200
    yield kbid


@pytest.fixture(scope="function")
async def dst_kb(
    nucliadb_writer_manager: AsyncClient,
):
    kbid = await create_kb(nucliadb_writer_manager)
    yield kbid



@pytest.mark.deploy_modes("standalone")
async def test_backup(
    nucliadb_reader: AsyncClient, src_kb: str, dst_kb: str
):
    context = ApplicationContext()
    await context.initialize()
    backup_id = "foo"
    await backup_kb(context, src_kb, backup_id)
