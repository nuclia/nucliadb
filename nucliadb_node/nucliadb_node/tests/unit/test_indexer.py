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

import asyncio
from functools import partial
from typing import AsyncIterator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from grpc import StatusCode
from grpc.aio import AioRpcError
from nucliadb_protos.nodewriter_pb2 import (
    IndexMessage,
    IndexMessageSource,
    OpStatus,
    TypeMessage,
)

from nucliadb_node.indexer import (
    ConcurrentShardIndexer,
    IndexNodeError,
    PriorityIndexer,
    WorkUnit,
)


class TestIndexerWorkUnit:
    def test_default_constructor_not_allowed(self):
        with pytest.raises(Exception) as exc:
            WorkUnit(index_message=Mock(), nats_msg=Mock(), mpu=Mock())
        assert "__init__ method not allowed" in str(exc.value)

    @pytest.mark.parametrize(
        "source",
        IndexMessageSource.keys(),
    )
    def test_from_msg_constructor(self, source):
        source = IndexMessageSource.Value(source)
        msg = gen_msg(source=source)
        work = WorkUnit.from_msg(msg)
        assert work.index_message.source == source

    @pytest.mark.parametrize("writer_seqid,processor_seqid", [(1, 2), (2, 1)])
    def test_priority_comparaisons_with_different_seqid(
        self,
        processor_seqid: int,
        writer_seqid: int,
    ):
        """Test all comparison operators to make sure ordering is properly
        implemented

        """
        processor_work_unit = WorkUnit.from_msg(
            gen_msg(seqid=processor_seqid, source=IndexMessageSource.PROCESSOR)
        )
        writer_work_unit = WorkUnit.from_msg(
            gen_msg(seqid=writer_seqid, source=IndexMessageSource.WRITER)
        )

        assert writer_work_unit < processor_work_unit
        assert writer_work_unit <= processor_work_unit  # type: ignore
        assert processor_work_unit > writer_work_unit
        assert processor_work_unit >= writer_work_unit  # type: ignore
        assert writer_work_unit != processor_work_unit
        assert processor_work_unit == processor_work_unit

    @pytest.mark.asyncio
    async def test_work_units_inside_an_asyncio_priority_queue(self):
        processor_work_unit = WorkUnit.from_msg(
            gen_msg(seqid=1, source=IndexMessageSource.PROCESSOR)
        )
        writer_work_unit_1 = WorkUnit.from_msg(
            gen_msg(seqid=2, source=IndexMessageSource.WRITER)
        )
        writer_work_unit_2 = WorkUnit.from_msg(
            gen_msg(seqid=3, source=IndexMessageSource.WRITER)
        )

        queue = asyncio.PriorityQueue()
        await queue.put(processor_work_unit)
        await queue.put(writer_work_unit_1)
        await queue.put(writer_work_unit_2)

        assert (await queue.get()) == writer_work_unit_1
        assert (await queue.get()) == writer_work_unit_2
        assert (await queue.get()) == processor_work_unit


class TestConcurrentShardIndexer:
    @pytest.fixture(autouse=True)
    def storage(self):
        with patch("nucliadb_node.indexer.get_storage"):
            yield

    @pytest.fixture(autouse=True)
    def message_progress_updater(self):
        mpu = Mock(return_value=AsyncMock())
        with patch("nucliadb_node.indexer.MessageProgressUpdater", new=mpu):
            yield mpu

    @pytest.fixture
    def writer(self):
        writer = AsyncMock()
        status = OpStatus()
        status.status = OpStatus.Status.OK
        status.detail = "Successful"
        writer.set_resource = AsyncMock(return_value=status)
        writer.delete_resource = AsyncMock(return_value=status)
        yield writer

    @pytest.fixture
    @pytest.mark.asyncio
    async def csi(self, writer) -> AsyncIterator[ConcurrentShardIndexer]:
        csi = ConcurrentShardIndexer(writer=writer, node_id="node-id")
        await csi.initialize()
        yield csi
        await csi.finalize()

    @pytest.mark.asyncio
    async def test_creates_an_indexer_and_a_task_per_shard(
        self,
        csi: ConcurrentShardIndexer,
    ):
        with patch("nucliadb_node.indexer.asyncio") as asyncio_mock, patch(
            "nucliadb_node.indexer.PriorityIndexer"
        ) as PriorityIndexer_mock:
            seqid = 1
            n_tasks = 5
            for i in range(n_tasks):
                shard_id = f"shard-{i}"
                for j in range(2):
                    msg = gen_msg(seqid=i, shard_id=shard_id)
                    csi.index_message_soon(msg)
                    seqid += 1
            assert asyncio_mock.create_task.call_count == n_tasks
            assert PriorityIndexer_mock.call_count == n_tasks

            # if we finish the concurrent shard indexer with mock subscriptions,
            # we'll have to wait for a timeout, as it'll wait all open tasks and
            # mocks won't finish
            csi.indexers.clear()

    @pytest.mark.asyncio
    async def test_indexing_adds_work_to_priority_indexer(
        self, csi: ConcurrentShardIndexer
    ):
        with patch("nucliadb_node.indexer.PriorityIndexer.index_soon") as index_soon:
            count = 5
            for i in range(count):
                csi.index_message_soon(gen_msg())
            assert index_soon.call_count == count

    @pytest.mark.asyncio
    async def test_priority_indexing_on_one_shard(self, csi: ConcurrentShardIndexer):
        messages = [
            gen_msg(seqid=1, source=IndexMessageSource.PROCESSOR),
            gen_msg(seqid=2, source=IndexMessageSource.PROCESSOR),
            gen_msg(seqid=3, source=IndexMessageSource.WRITER),
            gen_msg(seqid=4, source=IndexMessageSource.WRITER),
            gen_msg(seqid=5, source=IndexMessageSource.PROCESSOR),
        ]

        processed = []

        def new_indexer(*args, **kwargs):
            async def do_work_wrapper(original_do_work, work: WorkUnit):
                nonlocal processed
                await original_do_work(work)
                processed.append(work.seqid)

            indexer = PriorityIndexer(*args, **kwargs)
            original_do_work = indexer._do_work
            indexer._do_work = partial(do_work_wrapper, original_do_work)
            return indexer

        PriorityIndexer_mock = Mock(side_effect=new_indexer)
        with patch("nucliadb_node.indexer.PriorityIndexer", new=PriorityIndexer_mock):
            for msg in messages:
                csi.index_message_soon(msg)

        # wait until finish
        await asyncio.sleep(0.01)

        assert processed == [3, 4, 1, 2, 5]

    @pytest.mark.asyncio
    async def test_autoremove_finished_indexers(self, csi: ConcurrentShardIndexer):
        messages = [
            gen_msg(seqid=1, shard_id="shard-1"),
            gen_msg(seqid=2, shard_id="shard-2"),
            gen_msg(seqid=3, shard_id="shard-3"),
        ]
        for msg in messages:
            csi.index_message_soon(msg)
        assert len(csi.indexers) == 3
        await asyncio.sleep(0.01)
        assert len(csi.indexers) == 0


class TestPriorityIndexer:
    @pytest.fixture
    def successful_indexing(self):
        mock = AsyncMock()
        with patch("nucliadb_node.signals.successful_indexing", new=mock):
            yield mock

    @pytest.fixture(autouse=True)
    def storage(self):
        with patch("nucliadb_node.indexer.get_storage") as mock:
            yield mock

    @pytest.fixture
    def writer(self):
        writer = Mock()
        status = OpStatus()
        status.status = OpStatus.Status.OK
        writer.set_resource = AsyncMock(return_value=status)
        writer.delete_resource = AsyncMock(return_value=status)
        yield writer

    @pytest.fixture
    @pytest.mark.asyncio
    async def indexer(self, writer: AsyncMock, storage):
        indexer = PriorityIndexer(writer=writer, storage=storage)
        yield indexer

    def work(self):
        work = AsyncMock()
        work.__lt__ = lambda x, y: id(x) < id(y)
        work.index_message = IndexMessage()
        return work

    # Tests

    @pytest.mark.asyncio
    async def test_index_soon_inserts_into_queue(
        self,
        indexer: PriorityIndexer,
    ):
        indexer.index_soon(self.work())
        indexer.index_soon(self.work())

        assert indexer.work_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_work_until_finish_processes_everything(
        self, indexer: PriorityIndexer, successful_indexing, writer: AsyncMock
    ):
        total = 10
        for i in range(total):
            indexer.index_soon(self.work())
        assert indexer.work_queue.qsize() == total

        await indexer.work_until_finish()

        assert indexer.work_queue.qsize() == 0
        assert writer.set_resource.await_count == total
        assert successful_indexing.dispatch.await_count == total

    @pytest.mark.asyncio
    async def test_do_work_processes_a_work_unit(
        self, indexer: PriorityIndexer, writer: AsyncMock, successful_indexing
    ):
        work = self.work()
        work.seqid = 10
        await indexer._do_work(work)

        assert writer.set_resource.await_count == 1  # type: ignore
        assert successful_indexing.dispatch.await_count == 1
        assert successful_indexing.dispatch.await_args.args[0].seqid == 10

    @pytest.mark.asyncio
    async def test_node_writer_status_errors_are_handled(
        self, indexer: PriorityIndexer, writer: AsyncMock
    ):
        status = OpStatus()
        status.status = OpStatus.Status.ERROR
        status.detail = "node writer error"

        writer.set_resource.return_value = status
        with pytest.raises(IndexNodeError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.CREATION
            await indexer._index_message(pb)

        writer.delete_resource.return_value = status
        with pytest.raises(IndexNodeError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.DELETION
            await indexer._index_message(pb)

    @pytest.mark.asyncio
    async def test_node_writer_status_shard_not_found_errors_are_handled(
        self, indexer: PriorityIndexer, writer: AsyncMock
    ):
        status = OpStatus()
        status.status = OpStatus.Status.ERROR
        status.detail = 'status: NotFound, message: "Shard not found: Shard'

        writer.set_resource.return_value = status
        pb = IndexMessage()
        pb.typemessage = TypeMessage.CREATION
        await indexer._index_message(pb)

    @pytest.mark.asyncio
    async def test_node_writer_aio_rpc_errors_are_handled(
        self, indexer: PriorityIndexer, writer: AsyncMock
    ):
        writer.set_resource.side_effect = AioRpcError(
            code=StatusCode.UNKNOWN,
            initial_metadata=None,  # type: ignore
            trailing_metadata=None,  # type: ignore
            details="node writer error",
            debug_error_string="node writer error",
        )
        with pytest.raises(AioRpcError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.CREATION
            await indexer._index_message(pb)

        writer.delete_resource.side_effect = AioRpcError(
            code=StatusCode.UNKNOWN,
            initial_metadata=None,  # type: ignore
            trailing_metadata=None,  # type: ignore
            details="node writer error",
            debug_error_string="node writer error",
        )
        with pytest.raises(AioRpcError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.DELETION
            await indexer._index_message(pb)


def gen_msg(
    *,
    seqid: int = 1,
    shard_id: str = "test-shard-id",
    source: IndexMessageSource.ValueType = IndexMessageSource.WRITER,
):
    msg = AsyncMock()
    msg.reply = f"x.x.x.x.x.{seqid}"
    pb = IndexMessage()
    pb.shard = shard_id
    pb.source = source
    msg.data = pb.SerializeToString()
    return msg
