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
import base64
import uuid
from contextlib import contextmanager
from io import BytesIO
from unittest.mock import patch

import pytest

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.tasks import get_exports_consumer, get_imports_consumer


@pytest.fixture(scope="function")
async def src_kb(nucliadb_writer, nucliadb_manager):
    slug = uuid.uuid4().hex

    resp = await nucliadb_manager.post("/kbs", json={"slug": slug})
    assert resp.status_code == 201
    kbid = resp.json().get("uuid")

    for i in range(11):
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resources",
            json={
                "title": "Test",
                "thumbnail": "foobar",
                "icon": "application/pdf",
                "slug": f"test-{i}",
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
    yield kbid

    resp = await nucliadb_manager.delete(f"/kb/{kbid}")
    try:
        assert resp.status_code == 200
    except AssertionError:
        pass


@pytest.fixture(scope="function")
async def dst_kb(nucliadb_manager):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "dst_kb"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")
    yield uuid
    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    try:
        assert resp.status_code == 200
    except AssertionError:
        pass


@contextmanager
def set_standalone_mode_settings(standalone: bool):
    prev = cluster_settings.standalone_mode
    cluster_settings.standalone_mode = standalone
    yield
    cluster_settings.standalone_mode = prev


@pytest.fixture(scope="function")
def standalone_nucliadb():
    with set_standalone_mode_settings(True):
        yield


async def test_on_standalone_nucliadb(
    standalone_nucliadb,
    nucliadb_writer,
    nucliadb_reader,
    src_kb,
    dst_kb,
):
    await _test_export_import_kb_api(nucliadb_writer, nucliadb_reader, src_kb, dst_kb)


@pytest.fixture(scope="function")
def hosted_nucliadb():
    with patch("nucliadb.common.context.in_standalone_mode", return_value=False):
        with patch(
            "nucliadb.reader.api.v1.export_import.in_standalone_mode",
            return_value=False,
        ):
            with patch(
                "nucliadb.writer.api.v1.export_import.in_standalone_mode",
                return_value=False,
            ):
                with set_standalone_mode_settings(False):
                    yield


@pytest.fixture(scope="function")
async def context(hosted_nucliadb, natsd):
    context = ApplicationContext()
    await context.initialize()
    yield context
    await context.finalize()


@pytest.fixture(scope="function")
async def exports_consumer(context):
    consumer = get_exports_consumer()
    await consumer.initialize(context)


@pytest.fixture(scope="function")
async def imports_consumer(context):
    consumer = get_imports_consumer()
    await consumer.initialize(context)


async def test_on_hosted_nucliadb(
    hosted_nucliadb,
    nucliadb_writer,
    nucliadb_reader,
    src_kb,
    dst_kb,
    imports_consumer,
    exports_consumer,
):
    await _test_export_import_kb_api(nucliadb_writer, nucliadb_reader, src_kb, dst_kb)


async def _test_export_import_kb_api(nucliadb_writer, nucliadb_reader, src_kb, dst_kb):
    # Create export
    resp = await nucliadb_writer.post(f"/kb/{src_kb}/export", timeout=None)
    assert resp.status_code == 200
    export_id = resp.json()["export_id"]

    # Check for export
    await wait_for(nucliadb_reader, "export", src_kb, export_id)

    # Download export
    resp = await nucliadb_reader.get(f"/kb/{src_kb}/export/{export_id}", timeout=None)
    assert resp.status_code == 200
    export = BytesIO()
    for chunk in resp.iter_bytes():
        export.write(chunk)
    export.seek(0)

    # Upload import
    resp = await nucliadb_writer.post(
        f"/kb/{dst_kb}/import", content=export.getvalue(), timeout=None
    )
    assert resp.status_code == 200
    import_id = resp.json()["import_id"]

    # Check for import status
    await wait_for(nucliadb_reader, "import", dst_kb, import_id)

    # Finally, check that the KBs are equal
    await _check_kb(nucliadb_reader, src_kb)
    await _check_kb(nucliadb_reader, dst_kb)


async def wait_for(nucliadb_reader, type: str, kbid: str, id: str, max_retries=30):
    assert type in ("export", "import")
    finished = False
    for _ in range(max_retries):
        await asyncio.sleep(1)
        url = f"/kb/{kbid}/{type}/{id}/status"
        resp = await nucliadb_reader.get(url, timeout=None)
        assert resp.status_code == 200
        status = resp.json()["status"]
        assert status not in ("error", "failed")
        if resp.json()["status"] == "finished":
            finished = True
            break
    assert finished


async def _check_kb(nucliadb_reader, kbid):
    # Resource
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    body = resp.json()
    resources = body["resources"]
    assert len(resources) == 11
    for resource in resources:
        rid = resource["id"]
        assert resource["slug"].startswith("test-")
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
