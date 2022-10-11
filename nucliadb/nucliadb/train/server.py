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
from typing import Optional

from grpc import aio  # type: ignore
from grpc_health.v1 import health, health_pb2_grpc  # type: ignore

from nucliadb.train import logger
from nucliadb.train.servicer import TrainServicer
from nucliadb.train.settings import settings
from nucliadb_protos import train_pb2_grpc
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry, init_telemetry


async def start_grpc(service_name: Optional[str] = None):

    aio.init_grpc_aio()

    tracer_provider = get_telemetry(service_name)

    if tracer_provider is not None:
        await init_telemetry(tracer_provider)
        otgrpc = OpenTelemetryGRPC(f"{service_name}_grpc", tracer_provider)
        server = otgrpc.init_server()
    else:
        server = aio.server()

    servicer = TrainServicer()
    await servicer.initialize()
    health_servicer = health.aio.HealthServicer()  # type: ignore
    server.add_insecure_port(f"0.0.0.0:{settings.grpc_port}")

    train_pb2_grpc.add_TrainServicer_to_server(servicer, server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await server.start()
    logger.info(
        f"======= Train GRPC serving on http://0.0.0.0:{settings.grpc_port}/ ======"
    )

    async def finalizer():
        await servicer.finalize()
        await server.stop(grace=False)

    return finalizer
