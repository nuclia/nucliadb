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
#
import asyncio
import logging
from typing import Awaitable, Callable, Optional

from grpc import aio  # type: ignore
from grpc_health.v1 import health, health_pb2, health_pb2_grpc  # type: ignore

from nucliadb_utils.cache.nats import NatsPubsub
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.utilities import Utility, get_nats_manager, get_utility

logger = logging.getLogger(__name__)

_health_checks = []


def nats_manager_healthy() -> bool:
    nats_manager = get_nats_manager()
    if nats_manager is None:
        return True
    return nats_manager.healthy()


def nodes_health_check() -> bool:
    from nucliadb.common.cluster import manager
    from nucliadb.ingest.settings import DriverConfig, settings

    return len(manager.INDEX_NODES) > 0 or settings.driver == DriverConfig.LOCAL


def pubsub_check() -> bool:
    driver: Optional[PubSubDriver] = get_utility(Utility.PUBSUB)
    if driver is None:
        return True
    if isinstance(driver, NatsPubsub):
        if getattr(driver, "nc", None):
            return driver.nc.is_connected  # type: ignore
    return True


def register_health_checks(checks: list[Callable[[], bool]]) -> None:
    for check in checks:
        if check in _health_checks:
            continue
        _health_checks.append(check)


def unregister_health_check(check: Callable[[], bool]) -> None:
    if check in _health_checks:
        _health_checks.remove(check)


def unregister_all_checks() -> None:
    _health_checks.clear()


async def grpc_health_check(health_servicer) -> None:
    while True:
        for check in _health_checks:
            if not check():
                logger.info(f"Health check failed on {check.__name__}")
                await health_servicer.set(
                    "", health_pb2.HealthCheckResponse.NOT_SERVING
                )
                break
        else:
            await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)

        try:
            await asyncio.sleep(2)
        except (
            asyncio.CancelledError,
            asyncio.TimeoutError,
            KeyboardInterrupt,
        ):
            return


def setup_grpc_servicer(server) -> Callable[[], Awaitable[None]]:
    health_servicer = health.aio.HealthServicer()  # type: ignore
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)

    health_task = asyncio.create_task(grpc_health_check(health_servicer))

    async def finalizer():
        health_task.cancel()

    return finalizer


async def start_grpc_health_service(port: int) -> Callable[[], Awaitable[None]]:
    aio.init_grpc_aio()

    server = aio.server()
    server.add_insecure_port(f"0.0.0.0:{port}")

    health_check_finalizer = setup_grpc_servicer(server)

    await server.start()

    async def finalizer():
        await health_check_finalizer()
        await server.stop(grace=False)

    return finalizer
