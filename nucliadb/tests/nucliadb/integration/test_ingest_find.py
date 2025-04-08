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


import pytest
from httpx import AsyncClient

from nucliadb_protos.resources_pb2 import (
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldID,
    FieldType,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_protos.writer_pb2_grpc import WriterStub
from tests.utils import inject_message


@pytest.mark.deploy_modes("standalone")
async def test_field_update_deletes_old_vectors(
    nucliadb_reader: AsyncClient,
    nucliadb_writer: AsyncClient,
    nucliadb_ingest_grpc: WriterStub,
    standalone_knowledgebox,
):
    resp = await nucliadb_writer.post(
        f"/kb/{standalone_knowledgebox}/resources",
        json={
            "slug": "myresource",
            "title": "My Title",
            "summary": "My summary",
            "icon": "text/plain",
            "texts": {"text1": {"body": "Hello my dear friend"}},
        },
    )
    assert resp.status_code == 201
    rid = resp.json()["uuid"]

    bm = BrokerMessage()
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "Hello my dear friend"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    vector = Vector(start=0, end=len(etw.body.text), start_paragraph=0, end_paragraph=0)
    vector.vector.extend([1] * 512)
    evw.vectors.vectors.vectors.append(vector)
    bm.field_vectors.append(evw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # should get 1 result
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/find",
        json={"query": "Hello", "features": ["semantic"], "min_score": -1},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert len(body["resources"]) == 1

    # Now, the field is edited to something else and there are no vectors computed
    bm = BrokerMessage()
    bm.kbid = standalone_knowledgebox
    bm.uuid = rid

    field = FieldID(field_type=FieldType.TEXT, field="text1")
    etw = ExtractedTextWrapper()
    etw.body.text = "zxcvb"
    etw.field.CopyFrom(field)
    bm.extracted_text.append(etw)

    evw = ExtractedVectorsWrapper()
    evw.field.CopyFrom(field)
    bm.field_vectors.append(evw)

    await inject_message(nucliadb_ingest_grpc, bm)

    # should get no results with semantic search
    resp = await nucliadb_reader.post(
        f"/kb/{standalone_knowledgebox}/ask",
        json={
            "query": "zxcvb",
            "features": ["semantic"],
            "min_score": -1,
            "rag_strategies": [{"name": "neighbouring_paragraphs"}],
        },
        headers={"X-Synchronous": "True"},
    )
    assert resp.status_code == 200
    print(resp.text)
    body = resp.json()
    assert len(body["retrieval_results"]["resources"]) == 0
