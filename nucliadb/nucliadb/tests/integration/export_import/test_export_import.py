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

import pytest
from nucliadb_protos.writer_pb2 import BrokerMessage

from nucliadb.export_import import codecs
from nucliadb.export_import.context import ExporterContext
from nucliadb.export_import.exporter import export_kb
from nucliadb.export_import.importer import import_kb

pytestmark = pytest.mark.asyncio


@pytest.fixture()
async def exporter_context(knowledgebox, natsd, redis_config):
    context = ExporterContext()
    await context.initialize()
    yield context
    await context.finalize()


@pytest.fixture()
async def kbid_to_export(nucliadb_writer, knowledgebox):
    kbid = knowledgebox
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={"title": "Test", "thumbnail": "foobar"},
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

    return kbid


async def test_export_kb(exporter_context: ExporterContext, kbid_to_export):
    items_yielded = []
    async for chunk in export_kb(exporter_context, kbid_to_export):
        items_yielded.append(chunk)

    _test_exported_items(items_yielded, kbid=kbid_to_export)


def nwise(lst, n=1):
    while len(lst) >= n:
        to_yield = lst[:n]
        yield to_yield
        lst = lst[n:]


async def test_import_kb(
    exporter_context: ExporterContext, kbid_to_export, kbid_to_import
):
    export1 = []
    async for chunk in export_kb(exporter_context, kbid_to_export):
        export1.append(chunk)

    async def generator():
        for codex, _, data in nwise(export1, 3):
            yield codecs.CODEX(codex), data

    await import_kb(exporter_context, kbid_to_import, generator)

    export2 = []
    async for chunk in export_kb(exporter_context, kbid_to_import):
        export2.append(chunk)

    _test_exported_items(export2, kbid=kbid_to_import)


def _test_exported_items(items_yielded, kbid=None):
    assert len(items_yielded) == 12

    items = []
    for code_type, size, data in nwise(items_yielded, 3):
        items.append(codecs.CODEX(code_type.decode()))
        size_int = int.from_bytes(size, byteorder="big")
        assert len(data) == size_int

    assert items.count(codecs.CODEX.RESOURCE) == 1
    assert items.count(codecs.CODEX.BINARY) == 1
    assert items.count(codecs.CODEX.ENTITIES) == 1
    assert items.count(codecs.CODEX.LABELS) == 1

    bm_serialized = items_yielded[3]
    bm = BrokerMessage()
    bm.ParseFromString(bm_serialized)
    if kbid:
        assert bm.knowledgebox == kbid
