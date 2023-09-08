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
import os
import tarfile
import tempfile

import pytest

from nucliadb.export_import.context import ExporterContext
from nucliadb.export_import.exporter import export_kb

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
    kbid = kbid_to_export
    with tempfile.TemporaryDirectory() as temp_dir:
        export_file = await export_kb(exporter_context, kbid, temp_dir)

        # Check that the tar file has been created
        assert str(export_file) == f"{temp_dir}/{kbid}.export.tar.bz2"
        assert os.path.exists(export_file)

        # Check that it has the expected content
        with tarfile.open(export_file, mode="r:bz2") as tar:
            member_names = list([m.name for m in tar.getmembers()])
            assert "resources" in member_names
            member_names.pop(member_names.index("resources"))
            assert member_names[0].endswith("/f/f/file")
