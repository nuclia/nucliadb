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
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, Mock, patch

import pytest

from nucliadb.common.back_pressure.materializer import BackPressureMaterializer
from nucliadb.common.back_pressure.utils import BackPressureData, BackPressureException
from nucliadb.ingest.consumer.pull import PullWorker


class TestPullWorker:
    """
    It's a complex class so this might get a little messy with mocks

    It should be refactor at some point and these tests be rewritten/removed
    """

    @pytest.fixture()
    def processor(self):
        processor = AsyncMock()
        with patch("nucliadb.ingest.consumer.pull.Processor", return_value=processor):
            yield processor

    @pytest.fixture()
    def nats_conn(self):
        conn = MagicMock()
        conn.jetstream.return_value = AsyncMock()
        conn.drain = AsyncMock()
        conn.close = AsyncMock()
        yield conn

    @pytest.fixture()
    def worker(self, processor):
        yield PullWorker(
            driver=AsyncMock(),
            partition="1",
            storage=AsyncMock(),
            pull_time_error_backoff=100,
        )

    @pytest.fixture()
    async def back_pressure(self, nats_manager):
        materializer = BackPressureMaterializer(nats_manager)
        materializer.check_indexing = Mock(return_value=None)
        materializer.check_ingest = Mock(return_value=None)
        yield materializer

    async def test_back_pressure_check(
        self,
        worker: PullWorker,
        back_pressure,
        processor: AsyncMock,
        nats_conn: MagicMock,
    ):
        # If the worker does not have the back pressure, it should not be called
        worker.back_pressure = None

        await worker.back_pressure_check()

        back_pressure.check_ingest.assert_not_called()
        back_pressure.check_indexing.assert_not_called()

        # Check that the back pressure check is called when the worker has it
        worker.back_pressure = back_pressure

        await worker.back_pressure_check()

        back_pressure.check_ingest.assert_called_once()
        back_pressure.check_indexing.assert_called_once()

        # Check that it is retried when the back pressure exception is raised
        back_pressure.check_ingest.reset_mock()
        back_pressure.check_indexing.reset_mock()
        bpdata = BackPressureData(type="ingest", try_after=datetime.now(timezone.utc))
        back_pressure.check_ingest.side_effect = [BackPressureException(bpdata), None]

        await worker.back_pressure_check()

        assert back_pressure.check_ingest.call_count == 2

        # Other exceptions are not retried
        back_pressure.check_ingest.reset_mock()
        back_pressure.check_ingest.side_effect = [ValueError("test"), None]

        await worker.back_pressure_check()

        assert back_pressure.check_ingest.call_count == 1
