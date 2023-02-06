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
from grpc_health.v1 import health, health_pb2_grpc

from nucliadb_node import SERVICE_NAME
from nucliadb_node.reader import Reader  # type: ignore
from nucliadb_node.servicer import SidecarServicer
from nucliadb_node.settings import settings
from nucliadb_node.writer import Writer
from nucliadb_protos import nodewriter_pb2_grpc
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry


async def start_grpc(writer: Writer, reader: Reader):
    aio.init_grpc_aio()

    tracer_provider = get_telemetry(SERVICE_NAME)
    if tracer_provider is not None:
        telemetry_grpc = OpenTelemetryGRPC(
            f"{SERVICE_NAME}_grpc_server", tracer_provider
        )
        server = telemetry_grpc.init_server()
    else:
        server = aio.server()
    servicer = SidecarServicer(reader=reader, writer=writer)
    await servicer.initialize()
    health_servicer = health.aio.HealthServicer()  # type: ignore
    server.add_insecure_port(settings.sidecar_listen_address)

    nodewriter_pb2_grpc.add_NodeSidecarServicer_to_server(servicer, server)
    health_pb2_grpc.add_HealthServicer_to_server(health_servicer, server)
    await server.start()

    def finalizer():
        asyncio.create_task(servicer.finalize())
        asyncio.create_task(server.stop(grace=False))

    return finalizer
