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
from io import BytesIO

import pytest


@pytest.fixture(scope="function")
async def src_kb(knowledgebox, nucliadb_writer):
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={"title": "My resource"},
        headers={"X-Synchronous": "true"},
    )
    assert resp.status_code == 201
    yield knowledgebox


@pytest.fixture(scope="function")
async def dst_kb(nucliadb_manager):
    resp = await nucliadb_manager.post("/kbs", json={"slug": "dst_kb"})
    assert resp.status_code == 201
    uuid = resp.json().get("uuid")
    yield uuid
    resp = await nucliadb_manager.delete(f"/kb/{uuid}")
    assert resp.status_code == 200


async def test_export_import_kb_api(
    natsd,
    nucliadb_writer,
    nucliadb_reader,
    src_kb,
    dst_kb,
):
    export_response = await nucliadb_reader.get(f"/kb/{src_kb}/export")
    assert export_response.status_code == 200

    export = BytesIO()
    for chunk in export_response.iter_bytes():
        export.write(chunk)
    export.seek(0)

    import_response = await nucliadb_writer.post(
        f"/kb/{dst_kb}/import", content=export.getvalue()
    )
    assert import_response.status_code == 200

    # Check that the KBs are equal
    await _check_kb(nucliadb_reader, src_kb)
    await _check_kb(nucliadb_reader, dst_kb)


async def _check_kb(nucliadb_reader, kbid):
    resp = await nucliadb_reader.get(f"/kb/{kbid}/resources")
    assert resp.status_code == 200
    resources = resp.json()["resources"]
    assert len(resources) == 1
