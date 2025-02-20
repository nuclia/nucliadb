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

from nucliadb_models.agents.ingestion import ResourceAgentsResponse
from nucliadb_protos.resources_pb2 import FieldID, FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_ingestion_agents_on_resource(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    kbid = standalone_knowledgebox

    # Create a resource
    resp = await nucliadb_writer.post(
        f"/kb/{kbid}/resources",
        json={
            "title": "Resource 1",
            "slug": "resource-1",
            "texts": {
                "nietsche": {
                    "body": "Nietsche is a philosopher",
                },
            },
        },
    )
    assert resp.status_code == 201
    resource_id = resp.json()["uuid"]

    # Inject some field metadata and extracted text to it
    bm = BrokerMessage(kbid=kbid, uuid=resource_id, source=BrokerMessage.MessageSource.PROCESSOR)
    fieldid = FieldID(field_type=FieldType.TEXT, field="nietsche")
    bm.field_metadata.add()
    bm.field_metadata[0].field.CopyFrom(fieldid)
    bm.extracted_text.add()
    bm.extracted_text[0].field.CopyFrom(fieldid)
    bm.extracted_text[0].body.text = "Nietsche is a philosopher"
    await inject_message(nucliadb_ingest_grpc, bm)

    # Run agents on the resource
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/resource/{resource_id}/run-agents",
        json={},
    )
    assert resp.status_code == 200
    result = ResourceAgentsResponse.model_validate(resp.json())
    assert result.results.popitem()[1].applied_data_augmentation.changed

    # Run agents on the resource (by slug)
    resp = await nucliadb_reader.post(
        f"/kb/{kbid}/slug/resource-1/run-agents",
        json={},
    )
    assert resp.status_code == 200
    result = ResourceAgentsResponse.model_validate(resp.json())
    assert result.results.popitem()[1].applied_data_augmentation.changed
