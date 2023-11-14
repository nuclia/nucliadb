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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from nats.aio.client import Msg

from nucliadb_utils.nats.demux import (
    DemuxProcessor,
    DemuxWorker,
    NatsDemultiplexer,
    WorkOutcome,
    WorkUnit,
)


class TestNatsDemultiplexer:
    @pytest.fixture(autouse=True)
    def message_progress_updater(self):
        with patch("nucliadb_utils.nats.demux.MessageProgressUpdater"):
            yield

    @pytest.fixture()
    def processor(self) -> DemuxProcessor:
        def splitter(msg: Msg) -> tuple[str, Msg]:
            return ("test-split", msg)

        def process(work: Msg) -> bool:
            return True

        processor = Mock()
        processor.splitter = Mock(side_effect=splitter)
        processor.process = AsyncMock(side_effect=process)

        yield processor

    @pytest.fixture(scope="function")
    def nats_demux(self, processor: DemuxProcessor, message_progress_updater):
        yield NatsDemultiplexer(processor=processor, queue_klass=AsyncMock)

    @pytest.fixture
    def work_unit_done(self):
        mock = AsyncMock()
        with patch("nucliadb_utils.nats.demux.work_unit_done", new=mock):
            yield mock

    @pytest.fixture
    def msg(self):
        seqid = 1
        msg = Mock()
        msg.subject = "test-subject"
        msg.reply = f"x.x.x.x.x.{seqid}"
        msg.ack = AsyncMock()
        msg.nak = AsyncMock()
        yield msg

    @pytest.fixture
    def DemuxWorker(self):
        instance = AsyncMock()
        DemuxWorker = Mock(return_value=instance)
        with patch("nucliadb_utils.nats.demux.DemuxWorker", new=DemuxWorker):
            yield DemuxWorker

    @pytest.fixture
    def asyncio_mock(self):
        with patch("nucliadb_utils.nats.demux.asyncio") as asyncio_mock:
            yield asyncio_mock

    # Tests

    def test_handle_message_spawns_async_task(
        self, nats_demux: NatsDemultiplexer, asyncio_mock, msg
    ):
        nats_demux.handle_message_nowait(msg)
        assert asyncio_mock.create_task.called_once

    @pytest.mark.asyncio
    async def test_demux(
        self,
        nats_demux: NatsDemultiplexer,
        work_unit_done,
        msg,
        DemuxWorker,
        asyncio_mock,
    ):
        asyncio_mock.get_event_loop().create_future = AsyncMock(
            return_value=WorkOutcome(work_id="test-work", success=True)
        )

        await nats_demux.demux(msg)

        assert DemuxWorker().add_work.await_count == 1
        # called but not awaited, as we have mocked asyncio
        assert DemuxWorker().work_until_finish.call_count == 1
        assert msg.ack.await_count == 1
        assert msg.nak.await_count == 0

    @pytest.mark.asyncio
    async def test_demux_ignores_duplicated_work(
        self,
        nats_demux: NatsDemultiplexer,
        work_unit_done,
        msg,
        DemuxWorker,
        asyncio_mock,
    ):
        """Simulate NATS redeliver and validate duplicated work is not done."""

        async def fake_processing():
            await asyncio.sleep(0.05)
            return WorkOutcome(work_id="test-work", success=True)

        asyncio_mock.get_event_loop().create_future = AsyncMock(
            side_effect=fake_processing
        )

        await asyncio.gather(
            nats_demux.demux(msg),
            nats_demux.demux(msg),
        )

        assert DemuxWorker().add_work.await_count == 1
        # called but not awaited, as we have mocked asyncio
        assert DemuxWorker().work_until_finish.call_count == 1
        assert msg.ack.await_count == 1
        assert msg.nak.await_count == 0

    @pytest.mark.asyncio
    async def test_safe_demux_auto_nak(self, nats_demux: NatsDemultiplexer, msg):
        nats_demux.demux = AsyncMock(side_effect=Exception)  # type: ignore

        with pytest.raises(Exception):
            await nats_demux.safe_demux(msg)
        assert msg.nak.await_count == 1

    def tets_get_or_create_worker(self, nats_demux: NatsDemultiplexer):
        with patch("nucliadb_utils.nats.demux.DemuxWorker") as DemuxWorker:
            _, created = nats_demux.get_or_create_worker("worker_id-1")
            assert created
            assert DemuxWorker.call_count == 1

            worker_2, created = nats_demux.get_or_create_worker("worker_id-2")
            assert created
            assert DemuxWorker.call_count == 2

            worker_2_again, created = nats_demux.get_or_create_worker("worker_id-2")
            assert not created
            assert DemuxWorker.call_count == 2
            assert worker_2_again == worker_2

    def test_start_worker_creates_task_for_existing_worker(
        self, nats_demux: NatsDemultiplexer, asyncio_mock
    ):
        nats_demux.get_or_create_worker("worker_id-1")
        nats_demux.start_worker("worker_id-1")
        assert asyncio_mock.create_task.call_count == 1

    def test_start_worker_fails_for_unexistent_workers(
        self, nats_demux: NatsDemultiplexer, asyncio_mock
    ):
        with pytest.raises(KeyError):
            nats_demux.start_worker("worker_id-1")


class TestDemuxWorker:
    @pytest.fixture(scope="function")
    def queue(self):
        yield asyncio.Queue()

    @pytest.fixture
    def work_unit_done(self):
        mock = AsyncMock()
        with patch("nucliadb_utils.nats.demux.work_unit_done", new=mock):
            yield mock

    @pytest.fixture
    def demux_worker(self, queue: asyncio.Queue, work_unit_done):
        cb = AsyncMock(return_value=True)
        yield DemuxWorker(queue, cb)

    @pytest.fixture
    def evil_worker(self, queue: asyncio.Queue, work_unit_done):
        cb = AsyncMock(side_effect=Exception)
        yield DemuxWorker(queue, cb)

    # Tests

    @pytest.mark.asyncio
    async def test_add_work_inserts_into_queue(self, demux_worker: DemuxWorker, queue):
        await demux_worker.add_work(WorkUnit(work_id="test-1", work=Mock()))
        await demux_worker.add_work(WorkUnit(work_id="test-2", work=Mock()))

        assert demux_worker.work_queue.qsize() == 2

    @pytest.mark.asyncio
    async def test_do_work_processes_a_work_unit(
        self, demux_worker: DemuxWorker, work_unit_done
    ):
        work_unit = WorkUnit(work_id="test", work=Mock())
        await demux_worker.do_work(work_unit)

        assert demux_worker.cb.await_count == 1
        assert work_unit_done.dispatch.await_count == 1
        assert work_unit_done.dispatch.await_args.args[0].work_id == "test"
        assert work_unit_done.dispatch.await_args.args[0].success is True

    @pytest.mark.asyncio
    async def test_do_work_processes_handles_exceptions(
        self, evil_worker: DemuxWorker, work_unit_done
    ):
        work_unit = WorkUnit(work_id="test", work=Mock())
        await evil_worker.do_work(work_unit)
        assert evil_worker.cb.await_count == 1
        assert work_unit_done.dispatch.await_count == 1
        assert work_unit_done.dispatch.await_args.args[0].work_id == "test"
        assert work_unit_done.dispatch.await_args.args[0].error is not None
        assert isinstance(work_unit_done.dispatch.await_args.args[0].error, Exception)

    @pytest.mark.asyncio
    async def test_work_until_finish_processes_everything(
        self, demux_worker: DemuxWorker, work_unit_done
    ):
        total = 10
        for i in range(total):
            await demux_worker.add_work(WorkUnit(work_id=f"test-{i}", work=Mock()))
        assert demux_worker.work_queue.qsize() == total

        await demux_worker.work_until_finish()

        assert demux_worker.work_queue.qsize() == 0
        assert demux_worker.cb.await_count == total
        assert work_unit_done.dispatch.await_count == total
