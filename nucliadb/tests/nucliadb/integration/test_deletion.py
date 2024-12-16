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
import dataclasses

from httpx import AsyncClient

from nucliadb_models.search import SearchOptions
from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldComputedMetadataWrapper,
    FieldID,
    FieldType,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


async def test_paragraph_index_deletions(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_grpc: WriterStub,
    knowledgebox,
):
    @dataclasses.dataclass
    class FieldData:
        """
        Used for testing purposes only
        """

        field_id: str
        field_type: FieldType.ValueType
        text: str
        extracted_text: str
        vector: list[float]

    # Prepare data for a resource with title, summary and a text field
    original_text = "Original {field_id}"
    extracted_text = "Extracted text for {field_id}"
    title_field = FieldData(
        "title",
        FieldType.GENERIC,
        original_text.format(field_id="title"),
        extracted_text.format(field_id="title"),
        [1.0] * 512,
    )
    summary_field = FieldData(
        "summary",
        FieldType.GENERIC,
        original_text.format(field_id="summary"),
        extracted_text.format(field_id="summary"),
        [2.0] * 512,
    )
    text_field = FieldData(
        "text",
        FieldType.TEXT,
        original_text.format(field_id="text"),
        extracted_text.format(field_id="text"),
        [3.0] * 512,
    )

    # Create a resource with a simple text field
    resp = await nucliadb_writer.post(
        f"/kb/{knowledgebox}/resources",
        json={
            "title": title_field.text,
            "summary": summary_field.text,
            "texts": {
                text_field.field_id: {
                    "body": text_field.text,
                },
            },
        },
        timeout=None,
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    # Inject corresponding broker message as if it was coming from the processor
    bm = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    for field_data in (title_field, summary_field, text_field):
        pbfield = FieldID(
            field=field_data.field_id,
            field_type=field_data.field_type,
        )
        etw = ExtractedTextWrapper()
        etw.field.CopyFrom(pbfield)
        etw.body.text = field_data.extracted_text
        bm.extracted_text.append(etw)

        evw = ExtractedVectorsWrapper()
        evw.field.CopyFrom(pbfield)
        vector = Vector(
            start=0,
            end=len(field_data.extracted_text),
            start_paragraph=0,
            end_paragraph=len(field_data.extracted_text),
            vector=field_data.vector,
        )
        evw.vectors.vectors.vectors.append(vector)
        bm.field_vectors.append(evw)

        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.CopyFrom(pbfield)
        fcmw.metadata.metadata.paragraphs.append(Paragraph(start=0, end=len(field_data.extracted_text)))
        bm.field_metadata.append(fcmw)

    await inject_message(nucliadb_grpc, bm)

    # Check that searching for original texts does not return any results
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Original",
            "features": [SearchOptions.KEYWORD],
            "min_score": {"bm25": 0.0},
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 0

    # Check that searching for extracted texts returns all fields
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Extracted",
            "features": [SearchOptions.KEYWORD],
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    _rid, resource = resp_json["resources"].popitem()
    fields = resource["fields"]
    assert len(fields) == 3

    # Edit the field changing it's content
    text_field = FieldData(
        "text",
        FieldType.TEXT,
        "Modified text",
        "Modified coming from processor",
        [3.0] * 512,
    )

    resp = await nucliadb_writer.patch(
        f"/kb/{knowledgebox}/resource/{rid}",
        json={
            "texts": {
                text_field.field_id: {
                    "body": text_field.text,
                },
            }
        },
        timeout=None,
    )
    assert resp.status_code == 200

    # Inject broker message with the modified text
    bm = BrokerMessage(
        kbid=knowledgebox,
        uuid=rid,
        type=BrokerMessage.MessageType.AUTOCOMMIT,
        source=BrokerMessage.MessageSource.PROCESSOR,
    )

    for field_data in (title_field, summary_field, text_field):
        pbfield = FieldID(
            field=field_data.field_id,
            field_type=field_data.field_type,
        )
        etw = ExtractedTextWrapper()
        etw.field.CopyFrom(pbfield)
        etw.body.text = field_data.extracted_text
        bm.extracted_text.append(etw)

        evw = ExtractedVectorsWrapper()
        evw.field.CopyFrom(pbfield)
        vector = Vector(
            start=0,
            end=len(field_data.extracted_text),
            start_paragraph=0,
            end_paragraph=len(field_data.extracted_text),
            vector=field_data.vector,
        )
        evw.vectors.vectors.vectors.append(vector)
        bm.field_vectors.append(evw)

        fcmw = FieldComputedMetadataWrapper()
        fcmw.field.CopyFrom(pbfield)
        fcmw.metadata.metadata.paragraphs.append(Paragraph(start=0, end=len(field_data.extracted_text)))
        bm.field_metadata.append(fcmw)

    await inject_message(nucliadb_grpc, bm)

    await asyncio.sleep(0.5)  # wait for a while until reader gets updated

    # Check that searching for the first extracted text now doesn't return the
    # text field (as it has been modified)
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Extracted",
            "features": [SearchOptions.KEYWORD],
            "min_score": {"bm25": 0.0},
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    _rid, resource = resp_json["resources"].popitem()
    assert rid == _rid
    fields = resource["fields"]
    assert len(fields) == 2
    assert list(sorted(fields.keys())) == ["/a/summary", "/a/title"]

    # Check that searching for the modified text only returns the text field
    resp = await nucliadb_reader.post(
        f"/kb/{knowledgebox}/find",
        json={
            "query": "Modified",
            "features": [SearchOptions.KEYWORD],
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    _rid, resource = resp_json["resources"].popitem()
    assert rid == _rid
    fields = resource["fields"]
    assert len(fields) == 1
    assert len(fields["/t/text"]["paragraphs"]) == 1
    paragraph_id = list(fields["/t/text"]["paragraphs"].keys())[0]
    assert paragraph_id == f"{rid}/t/text/0-{len(text_field.extracted_text)}"
    fields["/t/text"]["paragraphs"][paragraph_id]["text"] == text_field.extracted_text
