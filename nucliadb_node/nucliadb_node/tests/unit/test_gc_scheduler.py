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

from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest
from grpc import StatusCode
from grpc.aio import AioRpcError
from nucliadb_protos.noderesources_pb2 import ShardId, ShardIds
from nucliadb_protos.nodewriter_pb2 import IndexMessage

from nucliadb_node.listeners import ShardGcScheduler
from nucliadb_node.signals import SuccessfulIndexingPayload, successful_indexing


class TestShardGcScheduler:
    @pytest.fixture
    def writer(self):
        writer = AsyncMock()

        shard_ids = ShardIds()
        shard_ids.ids.append(ShardId(id="shard-1"))
        shard_ids.ids.append(ShardId(id="shard-2"))
        writer.shards = AsyncMock(return_value=shard_ids)

        writer.garbage_collector = AsyncMock()

        yield writer

    @pytest.fixture
    @pytest.mark.asyncio
    async def gc_sched(self, writer):
        gc_sched = ShardGcScheduler(writer)
        await gc_sched.initialize()
        yield gc_sched
        await gc_sched.finalize()

    @pytest.mark.asyncio
    async def test_listener_registration_in_lifecycle_functions(self, writer):
        with patch("nucliadb_node.listeners.gc_scheduler.signals") as signals:
            gc_sched = ShardGcScheduler(writer)
            await gc_sched.initialize()
            assert signals.successful_indexing.add_listener.call_count == 1

            await gc_sched.finalize()
            assert signals.successful_indexing.remove_listener.call_count == 1

    @pytest.mark.asyncio
    async def test_successful_indexing_triggers_shard_changed_event(self, gc_sched):
        pb = IndexMessage()
        pb.shard = "myshard"
        payload = SuccessfulIndexingPayload(seqid=1, index_message=pb)

        sm = Mock()
        sm.lock = AsyncMock()
        sm_constructor = Mock(return_value=sm)
        with patch(
            "nucliadb_node.listeners.gc_scheduler.ShardManager", new=sm_constructor
        ):
            await successful_indexing.dispatch(payload)
            assert sm_constructor.call_count == 1
            assert sm.shard_changed_event.call_count == 1


class TestShardManager:
    @pytest.mark.asyncio
    async def test_garbage_collector_try_later_handling(self):
        from nucliadb_protos.nodewriter_pb2 import GarbageCollectorResponse

        from nucliadb_node.listeners.gc_scheduler import ShardManager

        try_later = GarbageCollectorResponse(
            status=GarbageCollectorResponse.Status.TRY_LATER
        )
        writer = AsyncMock(side_effect=try_later)

        with patch("nucliadb_node.listeners.gc_scheduler.asyncio"), patch(
            "nucliadb_node.listeners.gc_scheduler.capture_exception"
        ) as capture_exception:
            sm = ShardManager(shard_id="shard", writer=writer, gc_lock=AsyncMock())
            await sm.gc()

            assert writer.garbage_collector.await_count == 1
            assert capture_exception.call_count == 0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "grpc_error",
        [
            AioRpcError(
                code=StatusCode.NOT_FOUND,
                details="foobar",
                initial_metadata=MagicMock(),
                trailing_metadata=MagicMock(),
                debug_error_string="",
            ),
            AioRpcError(
                code=StatusCode.INTERNAL,
                details="Shard not found",
                initial_metadata=MagicMock(),
                trailing_metadata=MagicMock(),
                debug_error_string="",
            ),
        ],
    )
    async def test_garbage_collector_shard_not_found_handling(self, grpc_error):
        from nucliadb_node.listeners.gc_scheduler import ShardManager

        writer = Mock(garbage_collector=AsyncMock(side_effect=grpc_error))
        with patch("nucliadb_node.listeners.gc_scheduler.asyncio"), patch(
            "nucliadb_node.listeners.gc_scheduler.capture_exception"
        ) as capture_exception:
            sm = ShardManager(shard_id="shard", writer=writer, gc_lock=AsyncMock())
            await sm.gc()

            assert writer.garbage_collector.await_count == 1
            assert capture_exception.call_count == 0
