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

import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from nucliadb_utils import nats

pytestmark = pytest.mark.unit


class TestNatsConnectionManager:
    @pytest.fixture()
    def nats_conn(self):
        conn = MagicMock()
        conn.drain = AsyncMock()
        conn.close = AsyncMock()
        with patch("nucliadb_utils.nats.nats.connect", return_value=conn):
            yield conn

    @pytest.fixture()
    def js(self):
        conn = AsyncMock()
        with patch("nucliadb_utils.nats.get_traced_jetstream", return_value=conn):
            yield conn

    @pytest.fixture()
    def manager(self, nats_conn, js):
        yield nats.NatsConnectionManager(service_name="test", nats_servers=["test"])

    async def test_initialize(self, manager: nats.NatsConnectionManager, nats_conn):
        await manager.initialize()

        assert manager.nc == nats_conn

    async def test_lifecycle_finalize(
        self, manager: nats.NatsConnectionManager, nats_conn, js
    ):
        await manager.initialize()

        cb = AsyncMock()
        lost_cb = AsyncMock()
        await manager.subscribe(
            subject="subject",
            queue="queue",
            stream="stream",
            cb=cb,
            subscription_lost_cb=lost_cb,
            flow_control=True,
        )

        js.subscribe.assert_called_once_with(
            subject="subject",
            queue="queue",
            stream="stream",
            cb=cb,
            flow_control=True,
            config=None,
        )

        await manager.reconnected_cb()
        lost_cb.assert_called_once()

        await manager.finalize()

        nats_conn.drain.assert_called_once()
        nats_conn.close.assert_called_once()

    async def test_healthy(self, manager: nats.NatsConnectionManager):
        await manager.initialize()

        assert manager.healthy()

        manager._healthy = False
        assert not manager.healthy()

        manager._healthy = True
        manager._last_unhealthy = time.monotonic() - 100
        assert not manager.healthy()

        manager._last_unhealthy = None
        manager._nc.is_connected = False
        assert manager.healthy()
        assert manager._last_unhealthy is not None
