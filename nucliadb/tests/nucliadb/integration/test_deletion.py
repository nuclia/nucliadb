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




import dataclasses
from httpx import AsyncClient
from nucliadb_protos.resources_pb2 import ExtractedTextWrapper, FieldID, FieldType, ExtractedVectorsWrapper
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter
from nucliadb_protos.writer_pb2_grpc import WriterStub
import pytest


@pytest.mark.asyncio
#@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL", "STABLE"), indirect=True)
@pytest.mark.parametrize("knowledgebox", ("EXPERIMENTAL",), indirect=True)
async def test_concurrences(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    @dataclasses.dataclass
    class Field:
        field_id: str
        field_type: FieldType
        text: str
        extracted_text: str
        vector: list[float]

    # 
    original_text = "Original text uploaded by the user"
    extracted_text = "Extracted at processing time"
    extracted_vector = [1.0] * 512
    title = Field("title", FieldType.TEXT, original_text, extracted_text, extracted_vector)
    summary = Field("summary", FieldType.TEXT, original_text, extracted_text, extracted_vector)
    text = Field("text", FieldType.TEXT, original_text, extracted_text, extracted_vector)

    # Create a resource with a simple text field
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": title.text,
            "summary": summary.text,
            "texts": {
                text.field_id: {
                    "body": text.text,
                },
            }
        },
        timeout=None,
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Inject corresponding processed broker message
    bm = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    for field in (title, summary, text):
        pbfield = FieldID(
            field=field.field_id,
            field_type=field.field_type,
        )
        etw = ExtractedTextWrapper()
        etw.field.CopyFrom(pbfield)
        etw.body.text = field.extracted_text
        bm.extracted_text.append(etw)

        evw = ExtractedVectorsWrapper()
        evw.field.CopyFrom(pbfield)
        vector = Vector(
            start=0,
            end=len(field.extracted_text),
            start_paragraph=0,
            end_paragraph=len(field.extracted_text),
            vector=field.vector,
        )
        evw.vectors.vectors.vectors.append(vector)
        bm.field_vectors.append(evw)

    status: OpStatusWriter = await nucliadb_grpc.ProcessMessage([bm], timeout=None)
    assert status.status == OpStatusWriter.Status.OK

    # Check that the resource is in the database
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "extracted",
            "features": ["paragraph", "vector"],
            "vector": extracted_vector,
        },
        timeout=None,
    )
    assert resp.status_code == 200
    breakpoint()
    pass
