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

import pytest
from httpx import AsyncClient

from nucliadb.common.context import ApplicationContext
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource_with_title_paragraph, inject_message


async def create_resource(kbid: str, nucliadb_ingest_grpc: WriterStub):
    message = broker_resource_with_title_paragraph(kbid)
    await inject_message(nucliadb_ingest_grpc, message)
    return message.uuid


@pytest.mark.deploy_modes("standalone")
async def test_hidden_search(
    app_context,
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    nucliadb_writer_manager: AsyncClient,
    standalone_knowledgebox: str,
):
    resp = await nucliadb_writer_manager.patch(
        f"/kb/{standalone_knowledgebox}", json={"hidden_resources_enabled": True}
    )
    assert resp.status_code == 200

    r1 = await create_resource(standalone_knowledgebox, nucliadb_ingest_grpc)
    r2 = await create_resource(standalone_knowledgebox, nucliadb_ingest_grpc)

    # Both resources appear in searches
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/search")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r1, r2}

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/suggest?query=title")
    assert resp.status_code == 200
    assert {r["rid"] for r in resp.json()["paragraphs"]["results"]} == {r1, r2}

    # Hide r1
    resp = await nucliadb_writer.patch(
        f"/kb/{standalone_knowledgebox}/resource/{r1}", json={"hidden": True}
    )
    assert resp.status_code == 200
    await asyncio.sleep(0.5)

    # Only r2 appears on search
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/search")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r2}

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/suggest?query=title")
    assert resp.status_code == 200
    assert {r["rid"] for r in resp.json()["paragraphs"]["results"]} == {r2}

    # Unless show_hidden is passed, then both resources are returned
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/search?show_hidden=true")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r1, r2}

    resp = await nucliadb_reader.get(
        f"/kb/{standalone_knowledgebox}/suggest?query=title&show_hidden=true"
    )
    assert resp.status_code == 200
    assert {r["rid"] for r in resp.json()["paragraphs"]["results"]} == {r1, r2}

    # Test catalog ternary filter
    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/catalog")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r1, r2}

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/catalog?hidden=true")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r1}

    resp = await nucliadb_reader.get(f"/kb/{standalone_knowledgebox}/catalog?hidden=false")
    assert resp.status_code == 200
    assert resp.json()["resources"].keys() == {r2}


@pytest.fixture()
async def app_context(natsd, storage, nucliadb):
    ctx = ApplicationContext()
    await ctx.initialize()
    yield ctx
    await ctx.finalize()
