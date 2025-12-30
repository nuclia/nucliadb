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

import pytest
from httpx import AsyncClient

from nucliadb.common import datamanagers
from nucliadb_models.search import FindOptions
from nucliadb_protos.resources_pb2 import (
    FieldType,
    Paragraph,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message
from tests.utils.broker_messages import BrokerMessageBuilder
from tests.utils.dirty_index import wait_for_sync


@dataclasses.dataclass
class FieldData:
    """Helper to group field data"""

    field_id: str
    field_type: FieldType.ValueType
    text: str
    extracted_text: str
    vector: tuple[str, list[float]]


@pytest.mark.deploy_modes("standalone")
async def test_paragraph_index_deletions(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    # Prepare data for a resource with title, summary and a text field

    async with datamanagers.with_ro_transaction() as txn:
        vectorsets = [
            vs async for _, vs in datamanagers.vectorsets.iter(txn, kbid=standalone_knowledgebox)
        ]
    assert len(vectorsets) == 1
    vectorset_id = vectorsets[0].vectorset_id
    vector_dimension = vectorsets[0].vectorset_index_config.vector_dimension

    original_text = "Original {field_id}"
    extracted_text = "Extracted text for {field_id}"
    title_field = FieldData(
        "title",
        FieldType.GENERIC,
        original_text.format(field_id="title"),
        extracted_text.format(field_id="title"),
        (vectorset_id, [1.0] * vector_dimension),
    )
    summary_field = FieldData(
        "summary",
        FieldType.GENERIC,
        original_text.format(field_id="summary"),
        extracted_text.format(field_id="summary"),
        (vectorset_id, [2.0] * vector_dimension),
    )
    text_field = FieldData(
        "text",
        FieldType.TEXT,
        original_text.format(field_id="text"),
        extracted_text.format(field_id="text"),
        (vectorset_id, [3.0] * vector_dimension),
    )

    # Create a resource with a simple text field
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
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

    # Check that searching for original texts returns title and summary (text is
    # not indexed)
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Original",
            "features": [FindOptions.KEYWORD],
            "min_score": {"bm25": 0.0},
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    fields = resp_json["resources"][rid]["fields"]
    assert len(fields) == 2
    assert list(sorted(fields.keys())) == ["/a/summary", "/a/title"]

    # Inject corresponding broker message as if it was coming from the processor
    bmb = BrokerMessageBuilder(
        kbid=standalone_knowledgebox, rid=rid, source=BrokerMessage.MessageSource.PROCESSOR
    )
    bm = prepare_broker_message(bmb, title_field, summary_field, text_field)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()  # wait until changes are searchable

    # Check that searching for original texts does not return any results
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Original",
            "features": [FindOptions.KEYWORD],
            "min_score": {"bm25": 0.0},
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 0

    # Check that searching for extracted texts returns all fields
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Extracted",
            "features": [FindOptions.KEYWORD],
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    fields = resp_json["resources"][rid]["fields"]
    assert len(fields) == 3

    # Edit the field changing it's content
    text_field = FieldData(
        "text",
        FieldType.TEXT,
        "Modified text",
        "Modified coming from processor",
        (vectorset_id, [3.0] * 512),
    )

    resp = await nucliadb_writer.patch(
        f"/kb/{standalone_knowledgebox}/resource/{rid}",
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
    bmb = BrokerMessageBuilder(
        kbid=standalone_knowledgebox, rid=rid, source=BrokerMessage.MessageSource.PROCESSOR
    )
    bm = prepare_broker_message(bmb, title_field, summary_field, text_field)
    await inject_message(nucliadb_ingest_grpc, bm)
    await wait_for_sync()  # wait until changes are searchable

    # Check that searching for the first extracted text now doesn't return the
    # text field (as it has been modified)
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Extracted",
            "features": [FindOptions.KEYWORD],
            "min_score": {"bm25": 0.0},
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    fields = resp_json["resources"][rid]["fields"]
    assert len(fields) == 2
    assert list(sorted(fields.keys())) == ["/a/summary", "/a/title"]

    # Check that searching for the modified text only returns the text field
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={
            "query": "Modified",
            "features": [FindOptions.KEYWORD],
        },
        timeout=None,
    )
    assert resp.status_code == 200
    resp_json = resp.json()
    assert len(resp_json["resources"]) == 1
    fields = resp_json["resources"][rid]["fields"]
    assert len(fields) == 1
    assert len(fields["/t/text"]["paragraphs"]) == 1
    paragraph_id = next(iter(fields["/t/text"]["paragraphs"].keys()))
    assert paragraph_id == f"{rid}/t/text/0-{len(text_field.extracted_text)}"
    fields["/t/text"]["paragraphs"][paragraph_id]["text"] == text_field.extracted_text


def prepare_broker_message(
    bmb: BrokerMessageBuilder,
    title: FieldData,
    summary: FieldData,
    text_field: FieldData,
) -> BrokerMessage:
    title_builder = bmb.with_title(title.extracted_text)
    title_builder.with_extracted_vectors(
        [
            Vector(
                start=0,
                end=len(title.extracted_text),
                start_paragraph=0,
                end_paragraph=len(title.extracted_text),
                vector=title.vector[1],
            )
        ],
        title.vector[0],
    )

    summary_builder = bmb.with_summary(summary.extracted_text)
    summary_builder.with_extracted_vectors(
        [
            Vector(
                start=0,
                end=len(summary.extracted_text),
                start_paragraph=0,
                end_paragraph=len(summary.extracted_text),
                vector=summary.vector[1],
            )
        ],
        summary.vector[0],
    )

    field_builder = bmb.field_builder(text_field.field_id, text_field.field_type)
    field_builder.with_extracted_text(text_field.extracted_text)
    field_builder.with_extracted_paragraph_metadata(
        Paragraph(start=0, end=len(text_field.extracted_text))
    )
    field_builder.with_extracted_vectors(
        [
            Vector(
                start=0,
                end=len(text_field.extracted_text),
                start_paragraph=0,
                end_paragraph=len(text_field.extracted_text),
                vector=text_field.vector[1],
            )
        ],
        text_field.vector[0],
    )

    bm = bmb.build()

    return bm
