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

from nucliadb_protos import knowledgebox_pb2, writer_pb2
from nucliadb_protos.resources_pb2 import ExtractedVectorsWrapper, FieldType
from nucliadb_protos.utils_pb2 import Vector
from nucliadb_protos.writer_pb2 import BrokerMessage, IndexResource
from nucliadb_protos.writer_pb2_grpc import WriterStub


@pytest.mark.deploy_modes("component")
async def test_reindex_resource(dummy_nidx_utility, nucliadb_ingest_grpc: WriterStub, hosted_nucliadb):
    # Create a kb
    kbid = str(uuid4())
    result = await nucliadb_ingest_grpc.NewKnowledgeBoxV2(  # type: ignore
        writer_pb2.NewKnowledgeBoxV2Request(
            kbid=kbid,
            slug="test",
            title="My Title",
            vectorsets=[
                writer_pb2.NewKnowledgeBoxV2Request.VectorSet(
                    vectorset_id="my-semantic-model",
                    vector_dimension=2,
                )
            ],
        )
    )
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Create a resource with a field and some vectors
    bm = BrokerMessage()
    rid = "test1"
    field_id = "text1"
    field_type = FieldType.TEXT
    bm.uuid = rid
    bm.kbid = kbid
    bm.texts[field_id].body = "My text1"

    evw = ExtractedVectorsWrapper()
    evw.vectorset_id = "my-semantic-model"
    vec = Vector()
    vec.vector.extend([1.0, 1.0])
    evw.vectors.vectors.vectors.append(vec)
    evw.field.field = field_id
    evw.field.field_type = field_type
    bm.field_vectors.append(evw)

    await nucliadb_ingest_grpc.ProcessMessage([bm])  # type: ignore

    # Reindex it along with its vectors
    req = IndexResource(kbid=kbid, rid=rid, reindex_vectors=True)
    result = await nucliadb_ingest_grpc.ReIndex(req)  # type: ignore
