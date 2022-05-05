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

from grpc import aio  # type: ignore
from grpc_health.v1 import health, health_pb2_grpc  # type: ignore

from nucliadb_ingest import logger
from nucliadb_ingest.service.writer import WriterServicer
from nucliadb_ingest.settings import settings
from nucliadb_protos import writer_pb2_grpc


async def start_grpc():

    aio.init_grpc_aio()
    server = aio.server()

    servicer = WriterServicer()
    await servicer.initialize()
    health_servicer = health.aio.HealthServicer()
    server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")

    writer_pb2_grpc.add_WriterServicer_to_server(servicer, server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await server.start()
    logger.info(
        f"======= Ingest GRPC serving on http://0.0.0.0:{settings.grpc_port}/ ======"
    )

    def finalizer():
        asyncio.create_task(servicer.finalize())
        asyncio.create_task(server.stop(grace=False))

    return finalizer
