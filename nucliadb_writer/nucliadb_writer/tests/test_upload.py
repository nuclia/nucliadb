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

from nucliadb_models.resource import NucliaDBRoles
from nucliadb_writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCES_PREFIX
from nucliadb_writer.tus import UPLOAD


@pytest.mark.asyncio
async def test_upload(writer_api, knowledgebox_writer):
    kbid = knowledgebox_writer

    async with writer_api(roles=[NucliaDBRoles.WRITER]) as client:
        resp = await client.post(
            f"/{KB_PREFIX}/{kbid}/{UPLOAD}",
            headers={
                "X-Filename": base64.b64encode(b"testfile"),
                "X-Synchronous": "true",
                "Content-Type": "text/plain",
            },
            data=base64.b64encode(b"Test for /upload endpoint")
        )
        assert resp.status_code == 201
        body = resp.json()

        seqid = body["seqid"]
        rid = body["rid"]
        field_id = body["field_id"]

        assert str(seqid) == resp.headers["NDB-Seq"]
        assert rid == resp.headers["NDB-Resource"].split("/")[-1]
        assert field_id == resp.headers["NDB-Field"].split("/")[-1]
