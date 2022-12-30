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

from datetime import datetime

import pytest
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldType,
    Keyword,
    UserVector,
    UserVectorsWrapper,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage, ExportRequest, IndexResource

from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_export_resources(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    bm = BrokerMessage()
    bm.uuid = "test1"
    bm.slug = bm.basic.slug = "slugtest"
    bm.kbid = result.uuid
    bm.texts["text1"].body = "My text1"
    bm.files["file1"].file.uri = "http://nofile"
    bm.files["file1"].file.size = 0
    bm.files["file1"].file.source = CloudFile.Source.LOCAL
    bm.links["link1"].uri = "http://nolink"
    bm.datetimes["date1"].value.FromDatetime(datetime.now())
    bm.keywordsets["key1"].keywords.append(Keyword(value="key1"))
    evw = ExtractedVectorsWrapper()
    vec = Vector()
    vec.vector.extend([1.0, 1.0])
    evw.vectors.vectors.vectors.append(vec)
    evw.field.field = "text1"
    evw.field.field_type = FieldType.TEXT
    bm.field_vectors.append(evw)

    etw = ExtractedTextWrapper()
    etw.body.text = "My text"
    etw.field.field = "text1"
    etw.field.field_type = FieldType.TEXT
    bm.extracted_text.append(etw)
    bm.basic.title = "My Title"

    uvw = UserVectorsWrapper()
    uvw.field.field = "file1"
    uvw.field.field_type = FieldType.FILE
    uv = UserVector(vector=[1.0, 0.0], labels=["some", "labels"], start=1, end=2)
    uvw.vectors.vectors["vectorset1"].vectors["vect1"].CopyFrom(uv)
    bm.user_vectors.append(uvw)

    await stub.ProcessMessage([bm])  # type: ignore

    req = ExportRequest()
    req.kbid = result.uuid
    export: BrokerMessage
    found = False
    async for export in stub.Export(req):  # type: ignore
        assert found is False
        found = True
        assert export.basic.title == "My Title"
        assert export.slug == export.basic.slug == "slugtest"
        assert export.extracted_text[0].body.text == "My text"

        # Check that vectors are exported
        assert len(export.field_vectors) > 0
        assert len(export.user_vectors) > 0
    assert found

    req = ExportRequest()
    req.kbid = result.uuid
    found = False
    async for export in stub.Export(req):  # type: ignore
        assert found is False
        found = True
        assert export.basic.title == "My Title"
        assert export.texts["text1"].body == "My text1"
        assert export.extracted_text[0].body.text == "My text"
    assert found

    index_req = IndexResource()
    index_req.kbid = result.uuid
    index_req.rid = "test1"
    assert await stub.ReIndex(index_req)  # type: ignore
