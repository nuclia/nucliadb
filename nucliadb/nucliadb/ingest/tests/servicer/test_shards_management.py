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
from unittest import mock
from uuid import uuid4

import pytest
from nucliadb_protos.writer_pb2 import (
    CreateShadowShardRequest,
    DeleteShadowShardRequest,
)

from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.node import Node
from nucliadb.ingest.utils import get_driver
from nucliadb_protos import knowledgebox_pb2, writer_pb2_grpc


@pytest.mark.asyncio
async def test_create_and_delete_shadow_shard(grpc_servicer, fake_node):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)

    # Create a KB
    kbid = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test", forceuuid=kbid)
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Get current shards object
    driver = await get_driver()
    txn = await driver.begin()
    shards_object = await Node.get_all_shards(txn, kbid)
    await txn.abort()
    assert shards_object

    replica1 = shards_object.shards[0].replicas[0]
    replica2 = shards_object.shards[0].replicas[1]
    # There should not be shadow replicas by default
    assert not replica1.has_shadow
    assert not replica2.has_shadow
    rep1_id, rep1_node = replica1.shard.id, replica1.node
    _, rep2_node = replica2.shard.id, replica2.node

    # Create a shadow shard of rep1 onto the other node
    req = CreateShadowShardRequest()
    req.kbid = kbid
    req.replica.id = replica1.shard.id
    req.node = rep2_node

    resp = await stub.CreateShadowShard(req)
    assert resp.success

    # Check that the shadow replica has been updated at the shard object
    txn = await driver.begin()
    shards_object = await Node.get_all_shards(txn, kbid)
    await txn.abort()

    found = False
    for shard in shards_object.shards:
        for replica in shard.replicas:
            if replica.shard.id == rep1_id and replica.node == rep1_node:
                found = True
                assert replica.has_shadow
                assert replica.shadow_replica.shard.id
                assert replica.shadow_replica.node == rep2_node
            else:
                assert not replica.has_shadow
    assert found

    # Now delete it
    req = DeleteShadowShardRequest()
    req.kbid = kbid
    req.replica.id = replica1.shard.id
    resp = await stub.DeleteShadowShard(req)
    assert resp.success

    # Check that the shadow replica has been cleaned from the shard object
    txn = await driver.begin()
    shards_object = await Node.get_all_shards(txn, kbid)
    await txn.abort()

    found = False
    for shard in shards_object.shards:
        for replica in shard.replicas:
            if replica.shard.id == rep1_id and replica.node == rep1_node:
                found = True
                assert not replica.has_shadow
    assert found


@pytest.mark.asyncio
async def test_create_cleansup_on_error(grpc_servicer, fake_node):
    stub = writer_pb2_grpc.WriterStub(grpc_servicer.channel)
    # Create a KB
    kbid = str(uuid4())
    pb = knowledgebox_pb2.KnowledgeBoxNew(slug="test", forceuuid=kbid)
    pb.config.title = "My Title"
    result = await stub.NewKnowledgeBox(pb)
    assert result.status == knowledgebox_pb2.KnowledgeBoxResponseStatus.OK

    # Get current shards object
    driver = await get_driver()
    txn = await driver.begin()
    shards_object = await Node.get_all_shards(txn, kbid)
    await txn.abort()
    assert shards_object

    replica1 = shards_object.shards[0].replicas[0]

    # Clear sidecar mock
    node = NODES[replica1.node]
    node.sidecar.calls.clear()

    # Attempt to create a shadow shard of rep1 onto the same node
    req = CreateShadowShardRequest()
    req.kbid = kbid
    req.replica.id = replica1.shard.id
    req.node = replica1.node

    # Mock an error updating the shards object
    with mock.patch(
        "nucliadb.ingest.orm.node.update_shards_with_shadow_replica",
        side_effect=AttributeError,
    ):
        resp = await stub.CreateShadowShard(req)
        assert not resp.success

    # Check that the shadow shard was cleaned up after the error
    node = NODES[replica1.node]
    assert len(node.sidecar.calls["CreateShadowShard"]) == 1
    assert len(node.sidecar.calls["DeleteShadowShard"]) == 1
