# Copyright 2021 Bosutech XXI S.L.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import asyncio
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
        with patch(
            "nucliadb_utils.nats.get_traced_jetstream",
            return_value=conn,
        ):
            yield conn

    @pytest.fixture()
    def manager(self, nats_conn, js):
        yield nats.NatsConnectionManager(service_name="test", nats_servers=["test"])

    async def test_initialize(self, manager: nats.NatsConnectionManager, nats_conn):
        await manager.initialize()

        assert manager.nc == nats_conn

    async def test_lifecycle_finalize(self, manager: nats.NatsConnectionManager, nats_conn, js):
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
            manual_ack=True,
            flow_control=True,
            config=None,
        )

        await manager.reconnected_cb()

        # Wait a tiny bit to allow reconnect task to be scheduled
        await asyncio.sleep(0.01)
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
        manager._nc.is_connected = False  # type: ignore[ty:invalid-assignment]
        assert manager.healthy()
        assert manager._last_unhealthy is not None

        await manager.finalize()

    async def test_unsubscribe(self, manager: nats.NatsConnectionManager, nats_conn, js):
        await manager.initialize()

        cb = AsyncMock()
        lost_cb = AsyncMock()
        sub = await manager.subscribe(
            subject="subject",
            queue="queue",
            stream="stream",
            cb=cb,
            subscription_lost_cb=lost_cb,
            flow_control=True,
        )
        psub = await manager.pull_subscribe(
            subject="subject",
            stream="stream",
            cb=cb,
            subscription_lost_cb=lost_cb,
            durable="queue",
        )
        assert len(manager._subscriptions) == 1
        assert len(manager._pull_subscriptions) == 1

        await manager.unsubscribe(sub)
        await manager.unsubscribe(psub)

        sub.unsubscribe.assert_awaited_once()  # type: ignore[ty:unresolved-attribute]
        psub.unsubscribe.assert_awaited_once()  # type: ignore[ty:unresolved-attribute]
        assert len(manager._subscriptions) == 0
        assert len(manager._pull_subscriptions) == 0

        await manager.finalize()

        nats_conn.drain.assert_called_once()
        nats_conn.close.assert_called_once()

    async def test_reconnect_multiple_times(self, manager: nats.NatsConnectionManager, nats_conn, js):
        await manager.initialize()

        cb = AsyncMock()

        async def lost_cb():
            await asyncio.sleep(0.1)
            await manager.pull_subscribe(
                subject="subject",
                stream="stream",
                cb=cb,
                subscription_lost_cb=lost_cb,
                durable="queue",
            )

        async def fetch(**kwargs):
            await asyncio.sleep(0.1)
            return []

        psub = await manager.pull_subscribe(
            subject="subject",
            stream="stream",
            cb=cb,
            subscription_lost_cb=lost_cb,
            durable="queue",
        )
        psub.fetch = fetch  # type: ignore[ty:invalid-assignment]

        # We start with one subscription
        assert len(manager._pull_subscriptions) == 1

        # We reconnect two times, emulating nats client as closely as possible, including task cancellation
        # when a new callback is made
        task = asyncio.create_task(manager.reconnected_cb())
        await asyncio.sleep(0.01)
        task.cancel()
        task = asyncio.create_task(manager.reconnected_cb())
        await asyncio.sleep(0.21)
        await task

        # The lost_cb should have run and re-subscribed
        assert len(manager._pull_subscriptions) == 1

        await manager.finalize()

        nats_conn.drain.assert_called_once()
        nats_conn.close.assert_called_once()


async def test_message_progress_updater():
    in_progress = AsyncMock()
    msg = MagicMock(in_progress=in_progress, _ackd=False)

    async with nats.NatsMessageProgressUpdater(msg, 0.05):
        await asyncio.sleep(0.07)

    in_progress.assert_awaited_once()


async def test_message_progress_updater_does_not_update_ack():
    in_progress = AsyncMock()
    msg = MagicMock(in_progress=in_progress, _ackd=True)

    async with nats.NatsMessageProgressUpdater(msg, 0.05):
        await asyncio.sleep(0.07)

    in_progress.assert_not_awaited()
