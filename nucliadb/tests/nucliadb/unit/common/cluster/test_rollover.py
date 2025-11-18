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

from nucliadb.common.cluster import rollover
from nucliadb.common.datamanagers.rollover import RolloverState
from nucliadb_protos import knowledgebox_pb2, writer_pb2


@pytest.fixture()
def shards():
    yield writer_pb2.Shards(
        shards=[
            writer_pb2.ShardObject(
                shard="1",
                read_only=True,
            ),
            writer_pb2.ShardObject(
                shard="2",
                read_only=False,
            ),
        ],
        kbid="kbid",
        actual=1,
    )


@pytest.fixture()
def resource_ids():
    yield ["1", "2", "3"]


@pytest.fixture()
def resources_datamanager(resource_ids):
    mock = MagicMock()

    async def iterate_resource_ids(kbid):
        for id in resource_ids:
            yield id

    mock.iterate_resource_ids = iterate_resource_ids
    mock.get_resource_shard_id = AsyncMock()
    mock.get_resource_shard_id.return_value = "1"

    with (
        patch("nucliadb.common.cluster.rollover.datamanagers.resources", mock),
    ):
        yield mock


@pytest.fixture(scope="function")
def get_resource():
    res = MagicMock()
    res.uuid = "uuid"
    res.basic.modified.ToDatetime.return_value = datetime.now()
    res.kb = MagicMock(kbid="kbid")

    res.generate_index_message = AsyncMock()
    metadata = MagicMock()
    metadata.modified.ToDatetime.return_value = datetime.now()
    res.generate_index_message.return_value = metadata

    with patch("nucliadb.common.cluster.utils.Resource.get", return_value=res) as mock:
        yield mock


@pytest.fixture(scope="function")
def get_resource_index_message():
    with patch(
        "nucliadb.common.cluster.utils.index_message.get_resource_index_message",
        return_value=MagicMock(),
    ):
        yield


@pytest.fixture()
def cluster_datamanager(resource_ids, shards):
    mock = MagicMock()
    mock.get_kb_shards = AsyncMock()
    mock.get_kb_shards.return_value = shards
    mock.update_kb_shards = AsyncMock()

    with patch("nucliadb.common.cluster.rollover.datamanagers.cluster", mock):
        yield mock


@pytest.fixture()
def rollover_datamanager(resource_ids, cluster_datamanager):
    mock = MagicMock()
    mock.get_kb_rollover_shards = AsyncMock()
    mock.get_kb_rollover_shards.return_value = None
    mock.update_kb_rollover_shards = AsyncMock()
    mock.delete_kb_rollover_shard = AsyncMock()
    mock.delete_kb_rollover_shards = AsyncMock()
    mock.get_to_index = AsyncMock(side_effect=["1", None])
    mock.add_indexed = AsyncMock()
    mock.remove_to_index = AsyncMock()
    mock.get_indexed_data = AsyncMock(return_value=("1", 1))
    mock.remove_indexed = AsyncMock()
    mock.get_rollover_state = AsyncMock(return_value=RolloverState())
    mock.set_rollover_state = AsyncMock(return_value=RolloverState())
    mock.clear_rollover_state = AsyncMock(return_value=None)

    async def _mock_indexed_keys(kbid):
        yield "1"

    mock.iter_indexed_keys = _mock_indexed_keys

    def async_iterable(sequence):
        async def inner():
            for i in sequence:
                yield i

        return inner()

    with (
        patch("nucliadb.common.cluster.rollover.datamanagers.rollover", mock),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.with_transaction",
            return_value=AsyncMock(),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.with_ro_transaction",
            return_value=AsyncMock(),
        ),
        patch(
            "nucliadb.ingest.consumer.shard_creator.locking.distributed_lock",
            return_value=AsyncMock(),
        ),
        patch(
            "nucliadb.common.cluster.rollover.get_external_index_manager",
            return_value=None,
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.vectorsets.iter",
            return_value=async_iterable([]),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.atomic.kb.get_config",
            return_value=knowledgebox_pb2.KnowledgeBoxConfig(),
        ),
    ):
        yield mock


@pytest.fixture()
def app_context(rollover_datamanager, resources_datamanager, get_resource):
    mock = MagicMock()
    mock.shard_manager = MagicMock()
    mock.shard_manager.rollback_shard = AsyncMock()
    mock.shard_manager.add_resource = AsyncMock()
    mock.shard_manager.delete_resource = AsyncMock()
    mock.kv_driver = MagicMock()

    consumer_info = MagicMock()
    consumer_info.delivered.stream_seq = 0
    consumer_info.num_pending = 1
    mock.nats_manager.js = AsyncMock()
    mock.nats_manager.js.consumer_info.return_value = consumer_info
    yield mock


async def test_create_rollover_shards(
    app_context, shards: writer_pb2.Shards, rollover_datamanager, dummy_nidx_utility
):
    new_shards = await rollover.create_rollover_shards(app_context, "kbid")

    assert new_shards.kbid == "kbid"
    assert dummy_nidx_utility.api_client.NewShard.call_count == len(shards.shards)
    rollover_datamanager.update_kb_rollover_shards.assert_called_with(
        ANY, kbid="kbid", kb_shards=new_shards
    )


async def test_create_rollover_index_does_not_recreate(
    app_context, shards: writer_pb2.Shards, rollover_datamanager, dummy_nidx_utility
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
    )

    await rollover.create_rollover_index(app_context, "kbid")

    app_context.shard_manager.rollback_shard.assert_not_called()
    rollover_datamanager.update_kb_rollover_shards.assert_not_called()


async def test_index_to_rollover_index(
    app_context,
    rollover_datamanager,
    resources_datamanager,
    shards,
    resource_ids,
    get_resource_index_message,
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
    )
    await rollover.index_to_rollover_index(app_context, "kbid")
    rollover_datamanager.add_indexed.assert_called_with(
        ANY, kbid="kbid", resource_id="1", shard_id="1", modification_time=ANY
    )


async def test_index_to_rollover_index_handles_missing_shards(
    app_context, rollover_datamanager, resources_datamanager, shards, resource_ids
):
    rollover_datamanager.get_kb_rollover_shards.return_value = None
    with pytest.raises(rollover.UnexpectedRolloverError):
        await rollover.index_to_rollover_index(app_context, "kbid")


async def test_index_to_rollover_index_handles_missing_shard_id(
    app_context, rollover_datamanager, resources_datamanager, shards, resource_ids
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
    )
    resources_datamanager.get_resource_shard_id.return_value = None
    await rollover.index_to_rollover_index(app_context, "kbid")


async def test_index_to_rollover_index_handles_missing_res(
    app_context, rollover_datamanager, resources_datamanager, shards, resource_ids, get_resource
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
    )
    get_resource.return_value = None

    await rollover.index_to_rollover_index(app_context, "kbid")

    rollover_datamanager.remove_to_index.assert_called_with(ANY, kbid="kbid", resource="1")


async def test_cutover_index(app_context, rollover_datamanager, cluster_datamanager, shards):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
        resources_indexed=True,
    )
    await rollover.cutover_index(app_context, "kbid")

    cluster_datamanager.update_kb_shards.assert_called_with(ANY, kbid="kbid", shards=ANY)
    [app_context.shard_manager.rollback_shard.assert_any_call(shard) for shard in shards.shards]


async def test_cutover_index_missing(app_context, rollover_datamanager):
    rollover_datamanager.get_kb_rollover_shards.return_value = None

    with pytest.raises(rollover.UnexpectedRolloverError):
        await rollover.cutover_index(app_context, "kbid")


async def test_validate_indexed_data(
    app_context,
    rollover_datamanager,
    resources_datamanager,
    shards,
    resource_ids,
    get_resource_index_message,
    get_resource,
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
        resources_indexed=True,
        cutover_shards=True,
    )

    indexed_res = await rollover.validate_indexed_data(app_context, "kbid")
    assert len(indexed_res) == len(resource_ids)
    [get_resource.assert_any_call(ANY, kbid="kbid", rid=res_id) for res_id in resource_ids]


async def test_validate_indexed_data_handles_missing_res(
    app_context, rollover_datamanager, resources_datamanager, shards, resource_ids, get_resource
):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    rollover_datamanager.get_rollover_state.return_value = RolloverState(
        rollover_shards_created=True,
        resources_scheduled=True,
        resources_indexed=True,
        cutover_shards=True,
    )
    get_resource.return_value = None
    assert len(await rollover.validate_indexed_data(app_context, "kbid")) == 0


async def rollover_kb_index(app_context, rollover_datamanager, shards, resource_ids):
    rollover_datamanager.get_kb_rollover_shards.return_value = shards
    resource_ids.clear()

    await rollover.rollover_kb_index(app_context, "kbid")


async def test_clean_rollover_status(app_context, rollover_datamanager):
    await rollover.clean_rollover_status(app_context, "kbid")

    rollover_datamanager.clear_rollover_state.assert_called_once_with(ANY, kbid="kbid")
