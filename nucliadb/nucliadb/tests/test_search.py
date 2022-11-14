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
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_search_sc_2062(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/resource/{rid}")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=title")
    assert resp.status_code == 200

    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=summary")
    assert resp.status_code == 200
    assert len(resp.json()["paragraphs"]["results"]) == 1


def broker_resource_with_duplicates(knowledgebox):
    import uuid
    from datetime import datetime

    from nucliadb_protos.writer_pb2 import BrokerMessage

    from nucliadb.ingest.tests.vectors import V1
    from nucliadb_protos import resources_pb2 as rpb

    rid = str(uuid.uuid4())
    slug = f"{rid}slug1"

    bm: BrokerMessage = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        slug=slug,
        type=BrokerMessage.AUTOCOMMIT,
    )

    bm.basic.icon = "text/plain"
    bm.basic.title = "Title Resource"
    bm.basic.summary = "Summary of document"
    bm.basic.thumbnail = "doc"
    bm.basic.layout = "default"
    bm.basic.metadata.useful = True
    bm.basic.metadata.language = "es"
    bm.basic.created.FromDatetime(datetime.now())
    bm.basic.modified.FromDatetime(datetime.now())
    bm.origin.source = rpb.Origin.Source.WEB

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "My own text Ramon. This is great to be here. \n Where is my beer?"
    etw.field.field = "file"
    etw.field.field_type = rpb.FieldType.FILE
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Summary of document"
    etw.field.field = "summary"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    etw = rpb.ExtractedTextWrapper()
    etw.body.text = "Title Resource"
    etw.field.field = "title"
    etw.field.field_type = rpb.FieldType.GENERIC
    bm.extracted_text.append(etw)

    bm.files["file"].added.FromDatetime(datetime.now())
    bm.files["file"].file.source = rpb.CloudFile.Source.EXTERNAL

    fcm = rpb.FieldComputedMetadataWrapper()
    fcm.field.field = "file"
    fcm.field.field_type = rpb.FieldType.FILE
    dup_text = "My own text Ramon"
    p1 = rpb.Paragraph(
        start=0,
        end=len(dup_text),
        text=dup_text,
    )
    p1.start_seconds.append(0)
    p1.end_seconds.append(10)

    p2 = rpb.Paragraph(
        start=len(dup_text),
        end=len(dup_text) * 2,
        text=dup_text,
    )
    p2.start_seconds.append(10)
    p2.end_seconds.append(20)
    p2.start_seconds.append(20)
    p2.end_seconds.append(30)

    fcm.metadata.metadata.paragraphs.append(p1)
    fcm.metadata.metadata.paragraphs.append(p2)
    fcm.metadata.metadata.last_index.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_understanding.FromDatetime(datetime.now())
    fcm.metadata.metadata.last_extract.FromDatetime(datetime.now())

    bm.field_metadata.append(fcm)

    ev = rpb.ExtractedVectorsWrapper()
    ev.field.field = "file"
    ev.field.field_type = rpb.FieldType.FILE

    v1 = rpb.Vector()
    v1.start = 0
    v1.end = len(dup_text)
    v1.start_paragraph = 0
    v1.end_paragraph = len(dup_text)
    v1.vector.extend(V1)
    ev.vectors.vectors.vectors.append(v1)

    v2 = rpb.Vector()
    v2.start = len(dup_text)
    v2.end = len(dup_text) * 2
    v2.start_paragraph = len(dup_text)
    v2.end_paragraph = len(dup_text) * 2
    v2.vector.extend(V1)
    ev.vectors.vectors.vectors.append(v2)

    bm.field_vectors.append(ev)
    bm.source = BrokerMessage.MessageSource.WRITER
    return bm


async def inject_message(writer: WriterStub, message):
    await writer.ProcessMessage([message])  # type: ignore
    return


async def create_resource_with_duplicates(knowledgebox, writer: WriterStub):
    bm = broker_resource_with_duplicates(knowledgebox)
    await inject_message(writer, bm)
    return bm.uuid


@pytest.mark.asyncio
async def test_seach_filters_out_duplicates(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    await create_resource_with_duplicates(knowledgebox, nucliadb_grpc)

    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}/search?query=Ramon")
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 1
    assert len(content["sentences"]["results"]) == 1

    # It should return duplicates if specified
    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/search?query=Ramon&with_duplicates=true"
    )
    assert resp.status_code == 200
    content = resp.json()
    assert len(content["paragraphs"]["results"]) == 2
    assert len(content["sentences"]["results"]) == 2
