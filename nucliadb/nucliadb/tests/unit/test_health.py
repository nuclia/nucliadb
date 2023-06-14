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
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from grpc_health.v1 import health_pb2

from nucliadb import health
from nucliadb.common.cluster import manager
from nucliadb.ingest.settings import DriverConfig, settings

pytestmark = pytest.mark.asyncio


@pytest.fixture(autouse=True)
def register_checks():
    health.register_health_checks(
        [health.nats_manager_healthy, health.nodes_health_check, health.pubsub_check]
    )
    yield
    health.unregister_all_checks()


@pytest.fixture(autouse=True)
def nats_manager():
    mock = MagicMock()
    with patch("nucliadb.health.get_nats_manager", return_value=mock):
        yield mock


async def test_grpc_health_check():
    servicer = AsyncMock()
    with patch.object(manager, "INDEX_NODES", {"node1": "node1"}), patch.object(
        settings, "driver", DriverConfig.PG
    ):
        task = asyncio.create_task(health.grpc_health_check(servicer))
        await asyncio.sleep(0.05)

        servicer.set.assert_called_with("", health_pb2.HealthCheckResponse.SERVING)

        task.cancel()


async def test_health_check_fail():
    servicer = AsyncMock()
    with patch.object(manager, "INDEX_NODES", {}), patch.object(
        settings, "driver", DriverConfig.PG
    ):
        task = asyncio.create_task(health.grpc_health_check(servicer))
        await asyncio.sleep(0.05)

        servicer.set.assert_called_with("", health_pb2.HealthCheckResponse.NOT_SERVING)

        task.cancel()


async def test_health_check_fail_unhealthy_nats(nats_manager):
    nats_manager.healthy.return_value = False
    servicer = AsyncMock()
    with patch.object(manager, "INDEX_NODES", {"node1": "node1"}):  # has nodes
        task = asyncio.create_task(health.grpc_health_check(servicer))
        await asyncio.sleep(0.05)

        servicer.set.assert_called_with("", health_pb2.HealthCheckResponse.NOT_SERVING)

        task.cancel()
