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
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import BrokerMessage, OpStatusWriter

from nucliadb.ingest import SERVICE_NAME
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc
from nucliadb_utils.utilities import get_storage


@pytest.mark.asyncio
async def test_process_message_sets_storage_metadata_on_exports(
    grpc_servicer: IngestFixture,
):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test")
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
    bm.files["file1"].file.size = 10
    bm.files["file1"].file.source = CloudFile.Source.EXPORT
    bm.files["file1"].file.filename = "myfile.pdf"
    bm.files["file1"].file.content_type = "application/pdf"
    bm.files["file1"].file.bucket_name = f"test_{kbid}"
    bm.files["file1"].file.uri = f"kbs/{kbid}/r/test1/f/f/file1"

    # Process it
    resp = await stub.ProcessMessage([bm])  # type: ignore
    assert resp.status == OpStatusWriter.Status.OK

    # Check custom metadata has been set for the resource
    storage = await get_storage(service_name=SERVICE_NAME)
    bucket = storage.get_bucket_name(kbid)
    key = f"kbs/{kbid}/r/test1/f/f/file1"
    metadata = await storage.get_custom_metadata(bucket, key)
    assert metadata["CONTENT_TYPE"] == "application/pdf"
    assert metadata["SIZE"] == "10"
    assert metadata["FILENAME"] == "myfile.pdf"
