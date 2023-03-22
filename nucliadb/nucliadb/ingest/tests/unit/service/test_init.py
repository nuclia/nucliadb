import asyncio
from unittest.mock import MagicMock, patch

import pytest
from grpc_health.v1 import health_pb2

from nucliadb.ingest import service

pytestmark = pytest.mark.asyncio


async def test_health_check():
    servicer = MagicMock()
    with patch.object(service, "NODES", {"node1": "node1"}):
        task = asyncio.create_task(service.health_check(servicer))
        await asyncio.sleep(0.05)

        servicer.set.assert_called_with("", health_pb2.HealthCheckResponse.SERVING)

        task.cancel()


async def test_health_check_fail():
    servicer = MagicMock()
    with patch.object(service, "NODES", {}):
        task = asyncio.create_task(service.health_check(servicer))
        await asyncio.sleep(0.05)

        servicer.set.assert_called_with("", health_pb2.HealthCheckResponse.NOT_SERVING)

        task.cancel()
