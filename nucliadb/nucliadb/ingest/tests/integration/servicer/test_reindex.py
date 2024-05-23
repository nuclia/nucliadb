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
from uuid import uuid4

import pytest
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper, FieldType
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage, IndexResource

from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_reindex_resource(grpc_servicer, fake_node):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    # Create a kb
    kb_id = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(
        slug="test", forceuuid=kb_id, vector_dimension=512
    )
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Create a resource with a field and some vectors
    bm = BrokerMessage()
    rid = "test1"
    field_id = "text1"
    field_type = FieldType.TEXT
    bm.uuid = rid
    bm.kbid = result.uuid
    bm.texts[field_id].body = "My text1"

    evw = ExtractedVectorsWrapper()
    vec = Vector()
    vec.vector.extend([1.0, 1.0])
    evw.vectors.vectors.vectors.append(vec)
    evw.field.field = field_id
    evw.field.field_type = field_type
    bm.field_vectors.append(evw)

    await stub.ProcessMessage([bm])  # type: ignore

    # Reindex it along with its vectors
    req = IndexResource(kbid=kb_id, rid=rid, reindex_vectors=True)
    result = await stub.ReIndex(req)
