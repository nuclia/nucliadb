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
from contextlib import ExitStack, contextmanager
from datetime import datetime
from typing import Optional
from unittest.mock import ANY, AsyncMock, MagicMock, Mock, patch

import pytest

from nucliadb.common.cluster import manager, rollover
from nucliadb.common.cluster.index_node import IndexNode
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import writer_pb2

pytestmark = pytest.mark.asyncio


UNSET = object()


@contextmanager
def setup_mocks(
    *,
    kb_resources: list[Resource] = UNSET,  # type: ignore
    resource_shard_id: Optional[str] = UNSET,  # type: ignore
    kb_shards: Optional[writer_pb2.Shards] = UNSET,  # type: ignore
    kb_rollover_shards: Optional[writer_pb2.Shards] = UNSET,  # type: ignore
):
    """Setup a bunch of mocks to be able to unit test without calling a too many
    mocks each time or have too complex mock-everything fixtures

    """
    with ExitStack() as stack:
        # mock datamanagers transactions
        stack.enter_context(patch("nucliadb.common.cluster.rollover.datamanagers.with_transaction"))
        stack.enter_context(patch("nucliadb.common.cluster.rollover.datamanagers.with_ro_transaction"))
        stack.enter_context(
            patch("nucliadb.common.cluster.rollover.datamanagers.resources.with_ro_transaction")
        )
        stack.enter_context(
            patch("nucliadb.common.cluster.rollover.datamanagers.rollover.with_ro_transaction")
        )

        if kb_resources != UNSET:
            KnowledgeBox_mock = Mock()
            KnowledgeBox_mock.get = AsyncMock(side_effect=kb_resources + [None])

            stack.enter_context(
                patch(
                    "nucliadb.ingest.orm.knowledgebox.KnowledgeBox", Mock(return_value=KnowledgeBox_mock)
                )
            )

            kb_resource_ids = [resource.uuid for resource in kb_resources]
            stack.enter_context(
                patch(
                    "nucliadb.common.cluster.rollover.datamanagers.rollover.get_resource_to_index",
                    side_effect=kb_resource_ids + [None],
                )
            )

        if resource_shard_id != UNSET:
            stack.enter_context(
                patch(
                    "nucliadb.common.cluster.rollover.datamanagers.cluster.get_resource_shard_id",
                    return_value=resource_shard_id,
                )
            )

        if kb_shards != UNSET:
            stack.enter_context(
                patch(
                    "nucliadb.common.cluster.rollover.datamanagers.cluster.get_kb_shards",
                    return_value=kb_shards,
                )
            )

        if kb_rollover_shards != UNSET:
            stack.enter_context(
                patch(
                    "nucliadb.common.cluster.rollover.datamanagers.rollover.get_kb_rollover_shards",
                    return_value=kb_rollover_shards,
                )
            )

        yield


@pytest.fixture()
def available_nodes():
    nodes = {
        "0": IndexNode(id="0", address="node-0", available_disk=100, shard_count=0, dummy=True),
        "1": IndexNode(id="1", address="node-1", available_disk=100, shard_count=0, dummy=True),
        "2": IndexNode(id="2", address="node-2", available_disk=100, shard_count=0, dummy=True),
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
                read_only=True,
            ),
            writer_pb2.ShardObject(
                shard="2",
                replicas=[
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="3")),
                    writer_pb2.ShardReplica(shard=writer_pb2.ShardCreated(id="4")),
                ],
                read_only=False,
            ),
        ],
        kbid="kbid",
        actual=1,
    )


@pytest.fixture()
def resource():
    res = MagicMock()
    res.basic.modified.ToDatetime.return_value = datetime.now()

    brain = MagicMock()
    brain.modified.ToDatetime.return_value = datetime.now()
    res.generate_index_message = AsyncMock(return_value=Mock(brain=brain))

    yield res


@pytest.fixture()
def shard_manager():
    shard_manager = MagicMock()
    shard_manager.add_resource = AsyncMock()
    shard_manager.rollback_shard = AsyncMock()
    yield shard_manager


@pytest.fixture()
def mock_resource():
    resource = MagicMock()
    resource.uuid = "1"
    resource.basic.modified.ToDatetime.return_value = datetime.now()

    brain = MagicMock()
    brain.modified.ToDatetime.return_value = datetime.now()
    resource.generate_index_message = AsyncMock(return_value=Mock(brain=brain))

    yield resource


async def test_create_rollover_shards(
    available_nodes,
    shards: writer_pb2.Shards,
):
    app_context = Mock()
    app_context.shard_manager = Mock()

    with (
        setup_mocks(kb_shards=shards, kb_rollover_shards=None),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.update_kb_rollover_shards"
        ) as update_kb_rollover_shards,
    ):
        new_shards = await rollover.create_rollover_shards(app_context, "kbid")

        assert new_shards.kbid == "kbid"
        assert sum([len(node.writer.calls["NewShard"]) for node in available_nodes.values()]) == sum(
            [len(s.replicas) for s in shards.shards]
        )
        update_kb_rollover_shards.assert_called_with(ANY, kbid="kbid", kb_shards=new_shards)


async def test_create_rollover_shards_does_not_recreate(
    shards: writer_pb2.Shards,
):
    app_context = Mock()
    app_context.shard_manager = Mock()

    with (
        setup_mocks(kb_shards=shards, kb_rollover_shards=shards),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.update_kb_rollover_shards"
        ) as update_kb_rollover_shards,
    ):
        await rollover.create_rollover_shards(app_context, "kbid")

        app_context.shard_manager.rollback_shard.assert_not_called()
        update_kb_rollover_shards.assert_not_called()


async def test_index_rollover_shards(
    shard_manager,
    shards: writer_pb2.Shards,
    resource,
):
    shard_id = "1"
    with (
        setup_mocks(
            kb_resources=[resource],
            resource_shard_id=shard_id,
            kb_rollover_shards=shards,
        ),
        patch("nucliadb.common.cluster.rollover.datamanagers.rollover.add_indexed") as add_indexed_mock,
    ):
        app_context = MagicMock(shard_manager=shard_manager)
        kbid = "kbid"
        await rollover.index_rollover_shards(app_context, kbid)

        add_indexed_mock.assert_called_with(
            ANY, kbid=kbid, resource_id=resource.uuid, shard_id=shard_id, modification_time=1
        )


async def test_index_rollover_shards_handles_missing_shards():
    with setup_mocks(kb_rollover_shards=None):
        with pytest.raises(rollover.UnexpectedRolloverError):
            app_context = Mock()
            kbid = "kbid"
            await rollover.index_rollover_shards(app_context, kbid)


async def test_index_rollover_shards_handles_missing_shard_id(
    shards: writer_pb2.Shards,
    resource,
):
    with setup_mocks(
        kb_resources=[
            resource,
        ],
        resource_shard_id=None,
        kb_rollover_shards=shards,
    ):
        app_context = Mock()
        kbid = "kbid"
        await rollover.index_rollover_shards(app_context, kbid)


async def test_index_rollover_shards_handles_missing_resource(shards: writer_pb2.Shards):
    rid = "1"

    with (
        setup_mocks(kb_resources=[], resource_shard_id="1", kb_rollover_shards=shards),
        # rollover datamanager will return a resource that doesn't exist anymore
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.get_resource_to_index",
            side_effect=[rid, None],
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.remove_resource_to_index"
        ) as remove_resource_to_index_mock,
    ):
        app_context = Mock()
        kbid = "kbid"
        await rollover.index_rollover_shards(app_context, kbid)

        remove_resource_to_index_mock.assert_called_with(ANY, kbid=kbid, resource=rid)


async def test_cutover_shards(shard_manager, shards: writer_pb2.Shards):
    with (
        setup_mocks(kb_shards=shards, kb_rollover_shards=shards),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.cluster.update_kb_shards", new=AsyncMock()
        ) as update_kb_shards_mock,
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.delete_kb_rollover_shards",
            new=AsyncMock(),
        ) as delete_kb_rollover_shards_mock,
    ):
        app_context = MagicMock(shard_manager=shard_manager)
        kbid = "kbid"
        await rollover.cutover_shards(app_context, kbid)

        update_kb_shards_mock.assert_called_with(ANY, kbid=kbid, shards=ANY)
        delete_kb_rollover_shards_mock.assert_called_with(ANY, kbid=kbid)
        for shard in shards.shards:
            app_context.shard_manager.rollback_shard.assert_any_call(shard)


async def test_cutover_shards_missing():
    app_context = Mock()
    kbid = "kbid"

    with setup_mocks(kb_shards=shards, kb_rollover_shards=None):
        with pytest.raises(rollover.UnexpectedRolloverError):
            await rollover.cutover_shards(app_context, kbid)

    with setup_mocks(kb_shards=None, kb_rollover_shards=shards):
        with pytest.raises(rollover.UnexpectedRolloverError):
            await rollover.cutover_shards(app_context, kbid)


async def test_validate_indexed_data(
    shard_manager,
    shards: writer_pb2.Shards,
    resource,
):
    resource_ids = ["1", "2", "3"]

    KnowledgeBox_obj_mock = Mock()
    KnowledgeBox_obj_mock.get = AsyncMock(return_value=resource)
    KnowledgeBox_mock = Mock(return_value=KnowledgeBox_obj_mock)

    with (
        setup_mocks(kb_shards=shards, kb_rollover_shards=shards),
        # we need to patch KnowledgeBox so we can assert on the calls
        patch("nucliadb.ingest.orm.knowledgebox.KnowledgeBox", new=KnowledgeBox_mock),
        patch("nucliadb.common.cluster.rollover.KnowledgeBox", new=KnowledgeBox_mock),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.resources._iter_resource_slugs",
            async_iterator_from(resource_ids),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.iter_indexed_keys",
            async_iterator_from(["1"]),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.resources._get_resource_ids_from_slugs",
            side_effect=lambda kbid, slugs: slugs,
        ),
        # indexed data is older, so we'll have to repair the resources
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.get_indexed_data",
            AsyncMock(return_value=("1", 1)),
        ),
    ):
        app_context = Mock(shard_manager=shard_manager)
        indexed_res = await rollover.validate_indexed_data(app_context, "kbid")
        assert len(indexed_res) == len(resource_ids)
        for rid in resource_ids:
            KnowledgeBox_mock.assert_any_call(ANY, ANY, "kbid")
            KnowledgeBox_obj_mock.get.assert_any_call(rid)
            resource.generate_index_message.assert_any_call(reindex=False)


async def test_validate_indexed_data_handles_missing_res(
    shard_manager,
    shards: writer_pb2.Shards,
):
    resource_ids = ["1", "2", "3"]
    with (
        setup_mocks(kb_resources=[], kb_shards=shards, kb_rollover_shards=shards),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.resources._iter_resource_slugs",
            async_iterator_from(resource_ids),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.iter_indexed_keys",
            async_iterator_from(["1"]),
        ),
        # indexed data is older, so we'll have to repair the resources
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.get_indexed_data",
            AsyncMock(return_value=("1", 1)),
        ),
    ):
        app_context = Mock(shard_manager=shard_manager)
        indexed_res = await rollover.validate_indexed_data(app_context, "kbid")
        assert len(indexed_res) == 0


async def test_rollover_shards(
    available_nodes,
    shards,
    shard_manager,
    resource,
):
    with (
        setup_mocks(
            kb_resources=[resource], resource_shard_id="1", kb_shards=shards, kb_rollover_shards=shards
        ),
        patch(
            "nucliadb.common.cluster.rollover.locking.distributed_lock",
            return_value=AsyncMock(),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.resources.iterate_resource_ids",
            async_iterator_from([]),
        ),
        patch(
            "nucliadb.common.cluster.rollover.datamanagers.rollover.iter_indexed_keys",
            async_iterator_from(["1"]),
        ),
    ):
        app_context = Mock(shard_manager=shard_manager)
        await rollover.rollover_kb_shards(app_context, "kbid")


# Utils


def async_iterator_from(items):
    async def async_iterator(*args, **kwargs):
        nonlocal items
        for element in items:
            yield element

    return async_iterator
