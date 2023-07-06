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
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from nucliadb.common.cluster import manager, rollover
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb_protos import writer_pb2

pytestmark = pytest.mark.asyncio


@pytest.fixture()
def available_nodes():
    nodes = {
        "0": IndexNode(id="0", address="node-0", shard_count=0, dummy=True),
        "1": IndexNode(id="1", address="node-1", shard_count=30, dummy=True),
        "2": IndexNode(id="2", address="node-2", shard_count=40, dummy=True),
    }
    with patch.object(manager, "INDEX_NODES", new=nodes):
        yield nodes


@pytest.fixture()
def shards():
    yield writer_pb2.Shards(
        shards=[
            writer_pb2.ShardObject(
                shard="1",
                replicas=[
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="1")),
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="2")),
                ],
            ),
            writer_pb2.ShardObject(
                shard="2",
                replicas=[
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="3")),
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="4")),
                ],
            ),
        ],
        kbid="kbid",
        actual=1,
    )


@pytest.fixture()
def cluster_datamanager(shards):
    mock = AsyncMock()
    mock.get_kb_rollover_shards.return_value = None
    mock.get_kb_shards.return_value = shards
    with patch(
        "nucliadb.common.cluster.rollover.ClusterDataManager", return_value=mock
    ):
        yield mock


@pytest.fixture()
def resource_ids():
    yield ["1", "2", "3"]


@pytest.fixture()
def resources_datamanager(resource_ids):
    mock = AsyncMock()

    async def iterate_resource_ids(kbid):
        for id in resource_ids:
            yield id

    mock.iterate_resource_ids = iterate_resource_ids
    mock.get_resource_shard_id.return_value = "1"
    res = MagicMock()
    res.basic.modified.ToDatetime.return_value = datetime.now()
    mock.get_resource.return_value = res

    with patch(
        "nucliadb.common.cluster.rollover.ResourcesDataManager", return_value=mock
    ):
        yield mock


@pytest.fixture()
def app_context(cluster_datamanager, resources_datamanager, available_nodes):
    mock = MagicMock()
    mock.shard_manager = AsyncMock()
    mock.kv_driver = AsyncMock()
    consumer_info = MagicMock()
    consumer_info.delivered.stream_seq = 0
    mock.nats_manager.js.consumer_info.return_value = consumer_info
    yield mock


async def test_create_rollover_shards(
    app_context, available_nodes, shards: writer_pb2.Shards, cluster_datamanager
):
    new_shards = await rollover.create_rollover_shards(app_context, "kbid")

    assert new_shards.kbid == "kbid"
    assert len(available_nodes["0"].writer.calls["NewShard"]) == sum(
        [len(s.replicas) for s in shards.shards]
    )
    cluster_datamanager.update_kb_rollover_shards.assert_called_with("kbid", new_shards)


async def test_create_rollover_shards_rollback_existing(
    app_context, shards: writer_pb2.Shards, cluster_datamanager
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards

    await rollover.create_rollover_shards(app_context, "kbid")

    assert len(app_context.shard_manager.rollback_shard.mock_calls) == 2


async def test_index_rollover_shards(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards

    indexed_res = await rollover.index_rollover_shards(app_context, "kbid")
    assert len(indexed_res) == len(resource_ids)
    [
        resources_datamanager.get_resource_index_message.assert_any_call("kbid", res_id)
        for res_id in resource_ids
    ]


async def test_index_rollover_shards_handles_missing_shards(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = None
    with pytest.raises(rollover.UnexpectedRolloverError):
        await rollover.index_rollover_shards(app_context, "kbid")


async def test_index_rollover_shards_handles_missing_shard_id(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards
    resources_datamanager.get_resource_shard_id.return_value = None
    with pytest.raises(rollover.UnexpectedRolloverError):
        await rollover.index_rollover_shards(app_context, "kbid")


async def test_index_rollover_shards_handles_missing_res(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards
    resources_datamanager.get_resource_index_message.return_value = None
    assert len(await rollover.index_rollover_shards(app_context, "kbid")) == 0


async def test_cutover_shards(app_context, cluster_datamanager, shards):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards

    await rollover.cutover_shards(app_context, "kbid")

    cluster_datamanager.update_kb_shards.assert_called_with("kbid", ANY)
    [
        app_context.shard_manager.rollback_shard.assert_any_call(shard)
        for shard in shards.shards
    ]


async def test_cutover_shards_missing(app_context, cluster_datamanager):
    cluster_datamanager.get_kb_rollover_shards.return_value = None

    with pytest.raises(rollover.UnexpectedRolloverError):
        await rollover.cutover_shards(app_context, "kbid")


async def test_validate_indexed_data(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards

    indexed_res = await rollover.validate_indexed_data(
        app_context,
        "kbid",
        {res_id: ("1", datetime.min) for res_id in resource_ids},
    )
    assert len(indexed_res) == len(resource_ids)
    [
        resources_datamanager.get_resource_index_message.assert_any_call("kbid", res_id)
        for res_id in resource_ids
    ]


async def test_validate_indexed_data_handles_missing_res(
    app_context, cluster_datamanager, resources_datamanager, shards, resource_ids
):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards
    resources_datamanager.get_resource_index_message.return_value = None
    assert (
        len(
            await rollover.validate_indexed_data(
                app_context,
                "kbid",
                {res_id: ("1", datetime.min) for res_id in resource_ids},
            )
        )
        == 0
    )


async def test_rollover_shards(app_context, cluster_datamanager, shards, resource_ids):
    cluster_datamanager.get_kb_rollover_shards.return_value = shards
    resource_ids.clear()

    await rollover.rollover_shards(app_context, "kbid")
