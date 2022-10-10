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
import pytest
from httpx import AsyncClient

from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCES_PREFIX


@pytest.mark.asyncio
async def test_last_seqid_is_stored_in_resource(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    knowledgebox,
):

    resp = await nucliadb_writer.post(
        f"/{KB_PREFIX}/{knowledgebox}/{RESOURCES_PREFIX}",
        headers={"X-SYNCHRONOUS": "True"},
        json={
            "texts": {
                "textfield1": {"body": "Some text", "format": "PLAIN"},
            }
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    rid = data["uuid"]
    seqid = data["seqid"]

    # last_seqid should be returned on a resource get
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resource/{rid}")
    assert seqid == resp.json()["last_seqid"]

    # last_seqid should be returned when listing resources
    resp = await nucliadb_reader.get(f"/{KB_PREFIX}/{knowledgebox}/resources")
    resource_list = resp.json()["resources"]
    assert len(resource_list) > 0
    for resource in resource_list:
        assert "last_seqid" in resource
