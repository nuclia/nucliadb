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
from datetime import datetime
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import (
    CloudFile,
    Entity,
    ExtractedTextWrapper,
    ExtractedVectorsWrapper,
    FieldType,
    FileExtractedData,
    Keyword,
    LargeComputedMetadataWrapper,
    LinkExtractedData,
    UserVector,
    UserVectorsWrapper,
)
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import (
    BinaryData,
    BrokerMessage,
    ExportRequest,
    FileRequest,
    IndexResource,
    UploadBinaryData,
)

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc
from nucliadb_utils.utilities import get_storage


@pytest.mark.asyncio
async def test_export_resources(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug=f"test-{uuid4()}")
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

    lcmw = LargeComputedMetadataWrapper()
    lcmw.field.field = "text1"
    lcmw.field.field_type = FieldType.TEXT
    entity = Entity(token="token", root="root", type="type")
    lcmw.real.metadata.entities.append(entity)
    bm.field_large_metadata.append(lcmw)

    uvw = UserVectorsWrapper()
    uvw.field.field = "file1"
    uvw.field.field_type = FieldType.FILE
    uv = UserVector(vector=[1.0, 0.0], labels=["some", "labels"], start=1, end=2)
    uvw.vectors.vectors["vectorset1"].vectors["vect1"].CopyFrom(uv)
    bm.user_vectors.append(uvw)

    led = LinkExtractedData()
    led.metadata["foo"] = "bar"
    led.field = "link1"
    led.description = "My link is from wikipedia"
    led.title = "My Link"
    bm.link_extracted_data.append(led)

    fed = FileExtractedData()
    fed.language = "es"
    fed.metadata["foo"] = "bar"
    fed.icon = "image/png"
    fed.field = "file1"
    bm.file_extracted_data.append(fed)

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
        assert export.field_vectors == bm.field_vectors
        assert export.user_vectors == bm.user_vectors
        assert export.field_large_metadata == bm.field_large_metadata
        assert export.extracted_text == bm.extracted_text
        assert export.field_metadata == bm.field_metadata
        assert export.link_extracted_data == bm.link_extracted_data
        assert export.file_extracted_data == bm.file_extracted_data
    assert found

    index_req = IndexResource()
    index_req.kbid = result.uuid
    index_req.rid = "test1"
    assert await stub.ReIndex(index_req)  # type: ignore


@pytest.mark.asyncio
async def test_upload_download(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    # Create a KB
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug=f"test-{uuid4()}")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK
    kbid = result.uuid

    # Upload a file to it
    metadata = UploadBinaryData(count=0)
    metadata.metadata.size = 1
    metadata.metadata.kbid = kbid
    metadata.metadata.key = f"{kbid}/some/key"

    binary = base64.b64encode(b"Hola")
    data = UploadBinaryData(count=1)
    data.payload = binary

    async def upload_iterator():
        yield metadata
        yield data

    await stub.UploadFile(upload_iterator())  # type: ignore

    # Now download the file
    file_req = FileRequest()
    storage = await get_storage(service_name=SERVICE_NAME)
    file_req.bucket = storage.get_bucket_name(kbid)
    file_req.key = metadata.metadata.key

    downloaded = b""
    bindata: BinaryData
    async for bindata in stub.DownloadFile(file_req):  # type: ignore
        downloaded += bindata.data
    assert downloaded == binary


@pytest.mark.asyncio
async def test_export_file(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug=f"test-{uuid4()}")
    pb.config.title = "My Title"
    result: knowledgebox_pb2.NewKnowledgeBoxResponse = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK
    kbid = result.uuid

    # Create an exported bm with a file
    bm = BrokerMessage()
    bm.uuid = "test1"
    bm.slug = bm.basic.slug = "slugtest"
    bm.kbid = kbid
    bm.texts["text1"].body = "My text1"
    bm.files["file1"].file.size = 0
    bm.files["file1"].file.source = CloudFile.Source.EXPORT
    bm.files["file1"].file.bucket_name = "bucket_from_exported_kb"
    bm.files["file1"].file.uri = "/kbs/exported_kb/r/test1/f/f/file1"

    await stub.ProcessMessage([bm])  # type: ignore

    # Check that file bucket and uri were replaced
    req = ExportRequest()
    req.kbid = result.uuid
    export: BrokerMessage
    found = False
    async for export in stub.Export(req):  # type: ignore
        assert found is False
        found = True
        assert export.files["file1"].file.uri.startswith(f"kbs/{kbid}")
        assert kbid in export.files["file1"].file.bucket_name
    assert found
