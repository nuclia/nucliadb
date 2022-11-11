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
from nucliadb_protos.noderesources_pb2 import ShardCleaned
from nucliadb_protos.writer_pb2 import ShardCreated, ShardObject, ShardReplica, Shards

from nucliadb.ingest.service.writer import update_shards_with_updated_replica
from nucliadb.ingest.tests.fixtures import IngestFixture
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_clean_and_upgrade_kb_index(grpc_servicer: IngestFixture):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    kb_id = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test", forceuuid=kb_id)
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)  # type: ignore
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    req = knowledgebox_pb2.KnowledgeBoxID(uuid=kb_id)
    result = await stub.CleanAndUpgradeKnowledgeBoxIndex(req)  # type: ignore


def test_update_shards_pb_replica():
    shard1_rep1 = ShardReplica(
        node="node1",
        shard=ShardCreated(
            id="shard1rep1",
            document_service=ShardCreated.DOCUMENT_V0,
            paragraph_service=ShardCreated.PARAGRAPH_V0,
            relation_service=ShardCreated.RELATION_V0,
            vector_service=ShardCreated.VECTOR_V0,
        ),
    )
    shard1_rep2 = ShardReplica(
        node="node2",
        shard=ShardCreated(
            id="shard1rep2",
            document_service=ShardCreated.DOCUMENT_V0,
            paragraph_service=ShardCreated.PARAGRAPH_V0,
            relation_service=ShardCreated.RELATION_V0,
            vector_service=ShardCreated.VECTOR_V0,
        ),
    )
    shard2_rep1 = ShardReplica(
        node="node1",
        shard=ShardCreated(
            id="shard2rep1",
            document_service=ShardCreated.DOCUMENT_V0,
            paragraph_service=ShardCreated.PARAGRAPH_V0,
            relation_service=ShardCreated.RELATION_V0,
            vector_service=ShardCreated.VECTOR_V0,
        ),
    )
    shard2_rep2 = ShardReplica(
        node="node2",
        shard=ShardCreated(
            id="shard2rep2",
            document_service=ShardCreated.DOCUMENT_V0,
            paragraph_service=ShardCreated.PARAGRAPH_V0,
            relation_service=ShardCreated.RELATION_V0,
            vector_service=ShardCreated.VECTOR_V0,
        ),
    )
    shard1 = ShardObject(shard="shard1", replicas=[shard1_rep1, shard1_rep2])
    shard2 = ShardObject(shard="shard2", replicas=[shard2_rep1, shard2_rep2])

    shards = Shards(shards=[shard1, shard2])

    new_replica_info = ShardCleaned(
        document_service=ShardCreated.DOCUMENT_V1,
        relation_service=ShardCreated.RELATION_V1,
        vector_service=ShardCreated.VECTOR_V1,
        paragraph_service=ShardCreated.PARAGRAPH_V1,
    )

    update_shards_with_updated_replica(shards, "shard1rep1", new_replica_info)

    found = False
    for shard in shards.shards:
        for replica in shard.replicas:
            if replica.shard.id == "shard1rep1":
                assert replica.shard.document_service == ShardCreated.DOCUMENT_V1
                assert replica.shard.paragraph_service == ShardCreated.PARAGRAPH_V1
                assert replica.shard.vector_service == ShardCreated.VECTOR_V1
                assert replica.shard.relation_service == ShardCreated.RELATION_V1
                found = True
            else:
                assert replica.shard.document_service == ShardCreated.DOCUMENT_V0
                assert replica.shard.paragraph_service == ShardCreated.PARAGRAPH_V0
                assert replica.shard.vector_service == ShardCreated.VECTOR_V0
                assert replica.shard.relation_service == ShardCreated.RELATION_V0
    assert found
