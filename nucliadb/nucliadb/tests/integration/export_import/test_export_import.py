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
from io import BytesIO

import pytest

from nucliadb.export_import.context import KBExporterContext, KBImporterContext
from nucliadb.export_import.exporter import export_kb
from nucliadb.export_import.importer import ExportStream, import_kb

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def exporter_context(nucliadb, natsd):
    context = KBExporterContext()
    await context.initialize()
    yield context
    await context.finalize()


@pytest.fixture()
async def importer_context(nucliadb, natsd):
    context = KBImporterContext()
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


async def test_import_kb(
    exporter_context: KBExporterContext,
    importer_context: KBImporterContext,
    kbid_to_export,
    kbid_to_import,
    nucliadb_reader,
):
    assert kbid_to_export != kbid_to_import

    # Export kb
    export = BytesIO()
    async for chunk in export_kb(exporter_context, kbid_to_export):
        export.write(chunk)
    export.seek(0)

    # Import in another kb
    stream = ExportStream(export)
    await import_kb(importer_context, kbid_to_import, stream=stream)

    # Contents of kbs should be equal
    await _check_kb_contents(nucliadb_reader, kbid_to_export)
    await _check_kb_contents(nucliadb_reader, kbid_to_import)


async def _check_kb_contents(nucliadb_reader, kbid: str):
    # Resource
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    body = resp.json()
    resources = body["resources"]
    assert len(resources) == 1
    resource = resources[0]
    rid = resource["id"]
    assert resource["slug"] == "test"
    assert resource["title"] == "Test"
    assert resource["icon"] == "application/pdf"
    assert resource["thumbnail"] == "foobar"

    # File uploaded (metadata)
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resource/{rid}/file/file")
    assert resp.status_code == 200
    body = resp.json()
    field = body["value"]["file"]
    assert field["content_type"] == "text/plain"
    assert field["filename"] == "testfile"
    assert field["size"] == 36
    assert kbid in field["uri"]

    # File uploaded (content)
    resp = await nucliadb_reader.get(field["uri"])
    assert resp.status_code == 200
    assert base64.b64decode(resp.content) == b"Test for /upload endpoint"

    # Entities
    resp = await nucliadb_reader.get(f"/kb/{kbid}/entitiesgroups?show_entities=true")
    assert resp.status_code == 200
    body = resp.json()
    groups = body["groups"]
    assert len(groups) == 1
    group = groups["foo"]
    assert len(group["entities"]) == 1

    # Labels
    resp = await nucliadb_reader.get(f"/kb/{kbid}/labelsets")
    assert resp.status_code == 200
    body = resp.json()
    labelsets = body["labelsets"]
    assert len(labelsets) == 1
    labelset = labelsets["foo"]
    assert len(labelset["labels"]) == 1
