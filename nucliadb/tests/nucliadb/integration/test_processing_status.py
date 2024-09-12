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

from nucliadb_protos import resources_pb2 as rpb
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import broker_resource, inject_message


@pytest.mark.parametrize(
    "endpoint,expected_status,payload",
    [
        ("/kb/{kbid}/resource/{rid}/reprocess", 202, None),
        (
            "/kb/{kbid}/resource/{rid}/file/myfield/upload",
            201,
            {"content": b"this is a file field"},
        ),
    ],
)
@pytest.mark.asyncio
async def test_endpoint_set_resource_status_to_pending(
    endpoint,
    expected_status,
    payload,
    nucliadb_writer: AsyncClient,
    nucliadb_reader: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox: str,
):
    """
    - Create a resource with a status PROCESSED
    - Call endpoint that modifies the resource
    - Check that the status is set to PENDING
    """
    br = broker_resource(knowledgebox)
    br.basic.metadata.status = rpb.Metadata.Status.PROCESSED
    await inject_message(nucliadb_grpc, br)

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{br.uuid}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PROCESSED"

    kwargs = payload or {}
    resp = await nucliadb_writer.post(
        endpoint.format(kbid=knowledgebox, rid=br.uuid),
        **kwargs,
    )
    assert resp.status_code == expected_status

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{br.uuid}")
    assert resp.status_code == 200
    resp_json = resp.json()
    assert resp_json["metadata"]["status"] == "PENDING"
