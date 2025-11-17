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
from typing import AsyncIterator
from unittest import mock
from unittest.mock import patch

import pytest
from httpx import AsyncClient

from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.context import ApplicationContext
from nucliadb.export_import.tasks import get_exports_consumer, get_imports_consumer
from nucliadb.learning_proxy import LearningConfiguration
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb_utils.settings import indexing_settings


@pytest.fixture(scope="function")
async def src_kb(
    nucliadb_writer: AsyncClient, nucliadb_writer_manager: AsyncClient
) -> AsyncIterator[str]:
    slug = uuid.uuid4().hex

    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": slug})
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
        )
        assert resp.status_code == 201
        body = resp.json()
        rid = body["uuid"]

        content = b"Test for /upload endpoint"
        resp = await nucliadb_writer.post(
            f"/kb/{kbid}/resource/{rid}/file/file/upload",
            headers={
                "X-Filename": base64.b64encode(b"testfile").decode("utf-8"),
                "Content-Type": "text/plain",
            },
            content=base64.b64encode(content),
        )
        assert resp.status_code == 201

    # Create a resource with an externally hosted file
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Resource with externally hosted file",
            "thumbnail": "foobar",
            "icon": "application/pdf",
            "slug": "test-external",
            "files": {
                "externally-hosted-file": {
                    "file": {
                        "uri": "https://example.com/testfile",
                    }
                }
            },
        },
    )
    assert resp.status_code == 201

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

    try:
        # This can fail because the pg driver fixture might be destroyed by now
        resp = await nucliadb_writer_manager.delete(f"/kb/{kbid}")
        assert resp.status_code == 200
    except Exception:
        pass


@pytest.fixture(scope="function")
async def dst_kb(nucliadb_writer_manager: AsyncClient) -> AsyncIterator[str]:
    resp = await nucliadb_writer_manager.post("/kbs", json={"slug": "dst_kb"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")
    yield uuid

    try:
        # This can fail because the pg driver fixture might be destroyed by now
        resp = await nucliadb_writer_manager.delete(f"/kb/{uuid}")
        assert resp.status_code == 200
    except Exception:
        pass


@contextmanager
def set_standalone_mode_settings(standalone: bool):
    with patch.object(cluster_settings, "standalone_mode", standalone):
        yield


@pytest.mark.deploy_modes("standalone")
async def test_on_standalone_nucliadb(
    natsd,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    src_kb: str,
    dst_kb: str,
):
    await _test_export_import_kb_api(nucliadb_writer, nucliadb_reader, src_kb, dst_kb)


@pytest.fixture(scope="function")
def hosted_nucliadb(natsd):
    # FIXME
    # This is a very hacky way to run a component/hosted version of nucliadb
    # Instead of running all component fixtures, it runs the standalone
    # fixture overriding some settings.
    with (
        patch("nucliadb.common.context.in_standalone_mode", return_value=False),
        patch(
            "nucliadb.reader.api.v1.export_import.in_standalone_mode",
            return_value=False,
        ),
        patch(
            "nucliadb.writer.api.v1.export_import.in_standalone_mode",
            return_value=False,
        ),
        patch.object(indexing_settings, "index_jetstream_servers", [natsd]),
        set_standalone_mode_settings(False),
    ):
        yield


@pytest.fixture(scope="function")
async def context(hosted_nucliadb, natsd):
    context = ApplicationContext()
    await context.initialize()
    yield context
    await context.finalize()


@pytest.fixture(scope="function")
async def exports_consumer(context: ApplicationContext) -> AsyncIterator[NatsTaskConsumer]:
    consumer = get_exports_consumer()
    await consumer.initialize(context)
    yield consumer
    # XXX: finalize seems kind of broken
    # await consumer.finalize()


@pytest.fixture(scope="function")
async def imports_consumer(context: ApplicationContext) -> AsyncIterator[NatsTaskConsumer]:
    consumer = get_imports_consumer()
    await consumer.initialize(context)
    yield consumer
    # XXX: finalize seems kind of broken
    # await consumer.finalize()


@pytest.mark.deploy_modes("standalone")
async def test_on_hosted_nucliadb(
    hosted_nucliadb,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    src_kb: str,
    dst_kb: str,
    imports_consumer: NatsTaskConsumer,
    exports_consumer: NatsTaskConsumer,
):
    await _test_export_import_kb_api(nucliadb_writer, nucliadb_reader, src_kb, dst_kb)


async def _test_export_import_kb_api(
    nucliadb_writer: AsyncClient, nucliadb_reader: AsyncClient, src_kb: str, dst_kb: str
):
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
    resp = await nucliadb_writer.post(f"/kb/{dst_kb}/import", content=export.getvalue(), timeout=None)
    assert resp.status_code == 200
    import_id = resp.json()["import_id"]

    # Check for import status
    await wait_for(nucliadb_reader, "import", dst_kb, import_id)

    # Check that the KBs are equal
    await _check_kb(nucliadb_reader, src_kb)
    await _check_kb(nucliadb_reader, dst_kb)

    # Check learning config validation on import
    export.seek(0)
    await _test_learning_config_mismatch(nucliadb_writer, export, dst_kb)


@pytest.mark.deploy_modes("standalone")
async def test_export_and_create_kb_from_import_api(
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    src_kb: str,
):
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
    resp = await nucliadb_writer.post(f"/kbs/import", content=export.getvalue(), timeout=None)
    assert resp.status_code == 200
    dst_kb = resp.json()["kbid"]

    # Check that the KBs are equal
    await _check_kb(nucliadb_reader, src_kb)
    await _check_kb(nucliadb_reader, dst_kb)

    # Check learning config validation on import
    export.seek(0)
    await _test_learning_config_mismatch(nucliadb_writer, export, dst_kb)


@pytest.mark.deploy_modes("standalone")
async def _test_learning_config_mismatch(
    nucliadb_writer: AsyncClient,
    export: BytesIO,
    dst_kb: str,
):
    # Make sure that the import fails if learning configs don't match
    with mock.patch(
        "nucliadb.export_import.utils.get_learning_config",
        return_value=LearningConfiguration(
            semantic_model="unknown-model",
            semantic_threshold=0.5,
            semantic_vector_size=100,
            semantic_vector_similarity="cosine",
        ),
    ):
        resp = await nucliadb_writer.post(
            f"/kb/{dst_kb}/import", content=export.getvalue(), timeout=None
        )
        assert resp.status_code == 400
        assert (
            resp.json()["detail"]
            == "Cannot import. Semantic model mismatch: unknown-model != multilingual"
        )


async def wait_for(nucliadb_reader: AsyncClient, type: str, kbid: str, id: str, max_retries=30):
    assert type in ("export", "import")
    finished = False
    for _ in range(max_retries):
        await asyncio.sleep(1)
        url = f"/kb/{kbid}/{type}/{id}/status"
        resp = await nucliadb_reader.get(url, timeout=None)
        assert resp.status_code == 200
        status = resp.json()["status"]
        assert status != "error"
        if status == "finished":
            finished = True
            break
    assert finished


async def _check_kb(nucliadb_reader: AsyncClient, kbid: str):
    # Resource
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    body = resp.json()
    resources = body["resources"]
    assert len(resources) == 12
    for resource in resources:
        if resource["slug"] == "test-external":
            # Check externally hosted file was imported correctly below
            continue
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

    # Check externally hosted file
    resp = await nucliadb_reader.get(f"/kb/{kbid}/slug/test-external", params={"show": ["values"]})
    assert resp.status_code == 200
    body = resp.json()
    external_file = body["data"]["files"]["externally-hosted-file"]["value"]
    assert external_file["file"]["uri"] == "https://example.com/testfile"
    assert external_file["external"] is True

    # Labels
    resp = await nucliadb_reader.get(f"/kb/{kbid}/labelsets")
    assert resp.status_code == 200
    body = resp.json()
    labelsets = body["labelsets"]
    assert len(labelsets) == 1
    labelset = labelsets["foo"]
    assert len(labelset["labels"]) == 1
