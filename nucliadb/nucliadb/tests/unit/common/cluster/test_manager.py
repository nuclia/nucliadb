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
import asyncio
from unittest.mock import MagicMock

import pytest

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.exceptions import NoHealthyNodeAvailable
from nucliadb.common.cluster.settings import settings
from nucliadb_protos import writer_pb2


def test_should_create_new_shard():
    sm = manager.KBShardManager()
    low_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs - 1,
        "num_fields": 0,
    }
    high_para_counter = {
        "num_paragraphs": settings.max_shard_paragraphs + 1,
        "num_fields": 0,
    }
    assert sm.should_create_new_shard(**low_para_counter) is False
    assert sm.should_create_new_shard(**high_para_counter) is True

    low_fields_counter = {"num_fields": settings.max_shard_fields, "num_paragraphs": 0}
    high_fields_counter = {
        "num_fields": settings.max_shard_fields + 1,
        "num_paragraphs": 0,
    }
    assert sm.should_create_new_shard(**low_fields_counter) is False
    assert sm.should_create_new_shard(**high_fields_counter) is True


@pytest.fixture(scope="function")
async def fake_node():
    manager.INDEX_NODES.clear()
    yield manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        dummy=True,
    )
    manager.INDEX_NODES.clear()


async def test_standalone_node_garbage_collects(fake_node):
    mng = manager.StandaloneKBShardManager()

    mng.max_ops_before_checks = 0

    await mng.add_resource(
        writer_pb2.ShardObject(
            shard="123",
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ],
        ),
        resource=MagicMock(),
        txid=-1,
        partition=0,
        kb="kb",
    )

    await asyncio.sleep(0.05)
    assert len(fake_node.writer.calls["GC"]) == 1


def test_choose_node():
    manager.INDEX_NODES.clear()
    manager.add_index_node(
        id="node-0",
        address="nohost",
        shard_count=0,
        dummy=True,
    )
    manager.add_index_node(
        id="node-1",
        address="nohost",
        shard_count=0,
        dummy=True,
    )
    _, _, node_id = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ]
        )
    )
    assert node_id == "node-0"
    _, _, node_id = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-1"
                )
            ]
        )
    )
    assert node_id == "node-1"

    manager.INDEX_NODES.clear()
    manager.add_index_node(
        id="node-replica-0",
        address="nohost",
        shard_count=0,
        dummy=True,
        primary_id="node-0",
    )
    manager.add_index_node(
        id="node-replica-1",
        address="nohost",
        shard_count=0,
        dummy=True,
        primary_id="node-1",
    )
    _, _, node_id = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                )
            ]
        ),
        read_only=True,
    )
    assert node_id == "node-replica-0"
    _, _, node_id = manager.choose_node(
        writer_pb2.ShardObject(
            replicas=[
                writer_pb2.ShardReplica(
                    shard=writer_pb2.ShardCreated(id="123"), node="node-1"
                )
            ]
        ),
        read_only=True,
    )
    assert node_id == "node-replica-1"

    manager.remove_index_node("node-replica-0", "node-0")

    with pytest.raises(NoHealthyNodeAvailable):
        manager.choose_node(
            writer_pb2.ShardObject(
                replicas=[
                    writer_pb2.ShardReplica(
                        shard=writer_pb2.ShardCreated(id="123"), node="node-0"
                    )
                ]
            ),
            read_only=True,
        )

    manager.INDEX_NODES.clear()
