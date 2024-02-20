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
from uuid import uuid4

import pytest
from nucliadb_protos.writer_pb2 import BinaryData, FileRequest, UploadBinaryData

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc
from nucliadb_utils.utilities import get_storage


@pytest.mark.asyncio
async def test_upload_download(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)  # type: ignore

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
