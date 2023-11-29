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
from typing import AsyncIterator, Iterator
from unittest.mock import AsyncMock, Mock, patch

import pytest
from nats.aio.client import Msg
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
from nucliadb_utils.nats import MessageProgressUpdater


class TestIndexerWorkUnit:
    @pytest.fixture
    def processor_index_message(self) -> Iterator[IndexMessage]:
        index_message = IndexMessage()
        index_message.source = IndexMessageSource.PROCESSOR
        yield index_message

    @pytest.fixture
    def writer_index_message(self) -> Iterator[IndexMessage]:
        index_message = IndexMessage()
        index_message.source = IndexMessageSource.WRITER
        yield index_message

    @pytest.fixture
    def msg(self) -> Iterator[Msg]:
        seqid = 1
        msg = AsyncMock()
        msg.reply = f"x.x.x.x.x.{seqid}"
        yield msg

    @pytest.fixture
    def message_progress_updater(self) -> Iterator[MessageProgressUpdater]:
        mpu = AsyncMock()
        yield mpu

    @pytest.fixture
    def processor_work_unit(
        self,
        msg: Msg,
        processor_index_message: IndexMessage,
    ) -> Iterator[WorkUnit]:
        msg.data = processor_index_message.SerializeToString()
        yield WorkUnit.from_msg(msg)

    @pytest.fixture
    def writer_work_unit(
        self,
        msg: Msg,
        writer_index_message: IndexMessage,
    ) -> Iterator[WorkUnit]:
        msg.data = writer_index_message.SerializeToString()
        yield WorkUnit.from_msg(msg)

    def test_from_msg_constructor(
        self, msg: Msg, processor_index_message: IndexMessage
    ):
        msg.data = processor_index_message.SerializeToString()
        WorkUnit.from_msg(msg)

    @pytest.mark.parametrize("writer_seqid,processor_seqid", [(1, 2), (2, 1)])
    def test_priority_comparaisons_with_different_seqid(
        self,
        processor_work_unit: WorkUnit,
        writer_work_unit: WorkUnit,
        writer_seqid: int,
        processor_seqid: int,
    ):
        processor_work_unit.nats_msg.reply = f"x.x.x.x.x.{processor_seqid}"
        writer_work_unit.nats_msg.reply = f"x.x.x.x.x.{writer_seqid}"

        assert writer_work_unit < processor_work_unit
        assert processor_work_unit > writer_work_unit
        assert writer_work_unit != processor_work_unit
        assert processor_work_unit == processor_work_unit

    @pytest.mark.asyncio
    async def test_work_units_in_an_asyncio_priority_queue(
        self,
        processor_work_unit: WorkUnit,
        writer_work_unit: WorkUnit,
    ):
        processor_work_unit.nats_msg.reply = f"x.x.x.x.x.1"
        writer_work_unit.nats_msg.reply = f"x.x.x.x.x.2"

        queue: asyncio.PriorityQueue = asyncio.PriorityQueue()
        await queue.put(processor_work_unit)
        await queue.put(writer_work_unit)

        assert (await queue.get()) == writer_work_unit
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

    def gen_msg(
        self,
        *,
        seqid: int = 1,
        source: IndexMessageSource.ValueType = IndexMessageSource.WRITER,
    ):
        msg = AsyncMock()
        msg.reply = f"x.x.x.x.x.{seqid}"
        pb = IndexMessage()
        pb.shard = "test-shard-id"
        pb.source = source
        msg.data = pb.SerializeToString()
        return msg

    @pytest.fixture
    def msg(self) -> Iterator[Msg]:
        yield self.gen_msg(seqid=1)

    @pytest.fixture
    @pytest.mark.asyncio
    async def csi(self) -> AsyncIterator[ConcurrentShardIndexer]:
        csi = ConcurrentShardIndexer(writer=AsyncMock())
        await csi.initialize()
        yield csi
        await csi.finalize()

    @pytest.mark.asyncio
    async def test_concurrent_indexer_do_index(
        self, csi: ConcurrentShardIndexer, msg: Msg
    ):
        with patch("nucliadb_node.indexer.PriorityIndexer.index_soon") as index_soon:
            count = 10
            for i in range(count):
                await csi.index_message_soon(msg)
            assert index_soon.await_count == count

    @pytest.mark.asyncio
    async def test_concurrent_indexer_priority_indexing(
        self, csi: ConcurrentShardIndexer
    ):
        messages = [
            self.gen_msg(seqid=1, source=IndexMessageSource.PROCESSOR),
            self.gen_msg(seqid=2, source=IndexMessageSource.PROCESSOR),
            self.gen_msg(seqid=3, source=IndexMessageSource.WRITER),
            self.gen_msg(seqid=4, source=IndexMessageSource.WRITER),
            self.gen_msg(seqid=5, source=IndexMessageSource.PROCESSOR),
        ]

        processed = []

        def new_indexer(*args, **kwargs):
            async def do_work_mock(work: WorkUnit):
                nonlocal processed
                await asyncio.sleep(0.001)
                processed.append(work.seqid)

            indexer = PriorityIndexer(*args, **kwargs)
            indexer._do_work = AsyncMock(side_effect=do_work_mock)
            return indexer

        mock = Mock(side_effect=new_indexer)
        with patch("nucliadb_node.indexer.PriorityIndexer", new=mock):
            for msg in messages:
                await csi.index_message_soon(msg)

        # wait until finish
        await asyncio.sleep(0.01)

        assert processed == [3, 4, 1, 2, 5]


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
        await indexer.index_soon(self.work())
        await indexer.index_soon(self.work())

        assert indexer.work_queue.qsize() == 2

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
    async def test_work_until_finish_processes_everything(
        self, indexer: PriorityIndexer, successful_indexing, writer: AsyncMock
    ):
        total = 10
        for i in range(total):
            await indexer.index_soon(self.work())
        assert indexer.work_queue.qsize() == total

        await indexer.work_until_finish()

        assert indexer.work_queue.qsize() == 0
        assert writer.set_resource.await_count == total
        assert successful_indexing.dispatch.await_count == total

    @pytest.mark.asyncio
    async def test_indexer_creates_new_task_when_empty(
        self,
        indexer: PriorityIndexer,
    ):
        with patch("nucliadb_node.indexer.asyncio") as asyncio_mock:
            await indexer.index_soon(self.work())
            assert asyncio_mock.create_task.called_once

    @pytest.mark.asyncio
    async def test_node_writer_errors_are_managed(
        self, indexer: PriorityIndexer, writer: AsyncMock
    ):
        status = OpStatus()
        status.status = OpStatus.Status.ERROR
        status.detail = "node writer error"
        writer.set_resource.return_value = status
        writer.delete_resource.return_value = status

        with pytest.raises(IndexNodeError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.CREATION
            await indexer._index_message(pb)

        with pytest.raises(IndexNodeError):
            pb = IndexMessage()
            pb.typemessage = TypeMessage.DELETION
            await indexer._index_message(pb)

    @pytest.mark.asyncio
    async def test_indexing_error_flushes_queue(
        self, indexer: PriorityIndexer, writer: AsyncMock
    ):
        status = OpStatus()
        status.status = OpStatus.Status.ERROR
        status.detail = "node writer error"
        writer.set_resource.return_value = status
        writer.delete_resource.return_value = status

        with patch("nucliadb_node.indexer.asyncio"):
            await indexer.index_soon(self.work())
            await indexer.index_soon(self.work())
            await indexer.index_soon(self.work())
            assert indexer.work_queue.qsize() == 3

        indexer._do_work = AsyncMock(side_effect=indexer._do_work)
        await indexer.work_until_finish()

        assert indexer._do_work.await_count == 1
        assert indexer.work_queue.qsize() == 0
