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
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    FieldComputedMetadataWrapper,
    FieldType,
    Paragraph,
)
from nucliadb_protos.train_pb2 import GetSentencesRequest, TrainParagraph
from nucliadb_protos.train_pb2_grpc import TrainStub
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.asyncio
async def test_creation(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    nucliadb_train: TrainStub,
    knowledgebox,
):
    # PUBLIC API
    resp = await nucliadb_reader.get(f"/kb/{knowledgebox}")
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/labelset/ls1",
        json={"title": "Labelset 1", "labels": [{"text": "text", "title": "title"}]},
    )
    assert resp.status_code == 200

    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "slug": "myresource",
            "texts": {"text1": {"body": "My text"}},
        },
    )

    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # PROCESSING API

    bm = BrokerMessage()
    et = ExtractedTextWrapper()
    fm = FieldComputedMetadataWrapper()
    et.field.field = "text1"
    fm.field.field = "text1"
    et.field.field_type = FieldType.TEXT
    fm.field.field_type = FieldType.TEXT
    et.body.text = "My text"
    fm.metadata.metadata.language = "en"
    p1 = Paragraph()
    p1.start = 0
    p1.end = 7

    fm.metadata.metadata.paragraphs.append(p1)
    bm.extracted_text.append(et)
    bm.field_metadata.append(fm)
    bm.uuid = rid
    bm.kbid = knowledgebox

    async def iterate(value: BrokerMessage):
        yield value

    await nucliadb_grpc.ProcessMessage(iterate(bm))  # type: ignore

    resp = await nucliadb_reader.get(
        f"/kb/{knowledgebox}/resource/{rid}?show=extracted&show=values&extracted=text&extracted=metadata"
    )
    assert resp.status_code == 200
    assert (
        resp.json()["data"]["texts"]["text1"]["extracted"]["metadata"]["metadata"][
            "paragraphs"
        ][0]["end"]
        == 7
    )

    # ADD A LABEL

    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            "fieldmetadata": [
                {
                    "field": {
                        "field": "text1",
                        "field_type": "text",
                    },
                    "paragraphs": [
                        {
                            "key": f"{rid}/t/text1/0-7",
                            "classifications": [{"labelset": "ls1", "label": "title"}],
                        }
                    ],
                }
            ]
        },
    )
    assert resp.status_code == 200

    # TRAINING API
    request = GetSentencesRequest()
    request.kb.uuid = knowledgebox
    request.metadata.labels = True
    request.metadata.text = True
    paragraph: TrainParagraph
    async for paragraph in nucliadb_train.GetParagraphs(request):  # type: ignore
        assert paragraph.metadata.text == "My text"
        assert paragraph.metadata.labels.paragraph[0].label == "title"
