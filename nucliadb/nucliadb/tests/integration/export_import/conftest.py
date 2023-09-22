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
import base64
from contextlib import contextmanager

import pytest

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext


@contextmanager
def set_standalone_mode(value: bool):
    prev_value = cluster_settings.standalone_mode
    cluster_settings.standalone_mode = value
    yield
    cluster_settings.standalone_mode = prev_value


@pytest.fixture()
async def context(nucliadb, natsd):
    context = ApplicationContext()
    with set_standalone_mode(False):
        await context.initialize()
    yield context
    await context.finalize()


@pytest.fixture()
async def kbid_to_export(nucliadb_writer, knowledgebox):
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Test",
            "thumbnail": "foobar",
            "icon": "application/pdf",
            "slug": "test",
        },
        headers={"X-SYNCHRONOUS": "true"},
        timeout=None,
    )
    assert resp.status_code == 201
    body = resp.json()
    rid = body["uuid"]

    content = b"Test for /upload endpoint"
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resource/{rid}/file/file/upload",
        headers={
            "X-Filename": base64.b64encode(b"testfile").decode("utf-8"),
            "X-Synchronous": "true",
            "Content-Type": "text/plain",
        },
        content=base64.b64encode(content),
        timeout=None,
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

    return kbid


@pytest.fixture()
async def kbid_to_import(nucliadb_manager):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "kb_to_import"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")
    yield uuid
    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200
