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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
        with patch("nucliadb.ingest.consumer.pull.nats.connect", return_value=conn):
            yield conn

    @pytest.fixture()
    def worker(self, processor):
        yield PullWorker(
            driver=AsyncMock(),
            partition="1",
            storage=AsyncMock(),
            pull_time_error_backoff=100,
            zone="zone",
            nuclia_processing_cluster_url="nuclia_processing_cluster_url",
            nuclia_public_url="nuclia_public_url",
            audit=None,
            onprem=False,
        )
