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
from typing import Optional

from grpc import aio  # type: ignore
from grpc_health.v1 import health, health_pb2, health_pb2_grpc  # type: ignore

from nucliadb.ingest import logger
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.service.writer import WriterServicer
from nucliadb.ingest.settings import DriverConfig, settings
from nucliadb_protos import writer_pb2_grpc
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.grpc import get_traced_grpc_server


async def health_check(health_servicer):
    while True:
        if len(NODES) == 0 and settings.driver != DriverConfig.LOCAL:
            await health_servicer.set("", health_pb2.HealthCheckResponse.NOT_SERVING)
        else:
            await health_servicer.set("", health_pb2.HealthCheckResponse.SERVING)
        try:
            await asyncio.sleep(1)
        except (
            asyncio.CancelledError,
            asyncio.TimeoutError,
            KeyboardInterrupt,
        ):
            return


async def start_grpc(service_name: Optional[str] = None):
    aio.init_grpc_aio()

    await setup_telemetry(service_name or "ingest")
    server = get_traced_grpc_server(
        service_name or "ingest",
        max_receive_message=settings.max_receive_message_length,
    )

    servicer = WriterServicer()
    await servicer.initialize()
    health_servicer = health.aio.HealthServicer()  # type: ignore
    server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")

    writer_pb2_grpc.add_WriterServicer_to_server(servicer, server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await server.start()

    logger.info(
        f"======= Ingest GRPC running on http://0.0.0.0:{settings.grpc_port}/ ======"
    )

    health_task = asyncio.create_task(health_check(health_servicer))

    async def finalizer():
        health_task.cancel()
        await servicer.finalize()
        await server.stop(grace=False)

    return finalizer
