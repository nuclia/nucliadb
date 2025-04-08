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
import logging
import os
from dataclasses import dataclass
from os.path import dirname
from typing import AsyncIterator

import pytest

from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.service.writer import WriterServicer
from nucliadb.standalone.settings import Settings
from nucliadb_protos import writer_pb2_grpc
from nucliadb_protos.writer_pb2_grpc import WriterStub
from nucliadb_utils.grpc import get_traced_grpc_channel, get_traced_grpc_server
from nucliadb_utils.storages.storage import Storage
from tests.ndbfixtures import SERVICE_NAME

logger = logging.getLogger(__name__)

INGEST_TESTS_DIR = os.path.abspath(os.path.join(dirname(__file__), "..", "ingest"))


@dataclass
class IngestGrpcServer:
    host: str
    port: int

    @property
    def address(self):
        return f"{self.host}:{self.port}"


# Main fixtures


@pytest.fixture(scope="function")
async def component_nucliadb_ingest_grpc(
    ingest_grpc_server: IngestGrpcServer,
) -> AsyncIterator[WriterStub]:
    channel = get_traced_grpc_channel(ingest_grpc_server.address, SERVICE_NAME)
    stub = WriterStub(channel)
    yield stub
    await channel.close(grace=None)


@pytest.fixture(scope="function")
async def standalone_nucliadb_ingest_grpc(nucliadb: Settings) -> AsyncIterator[WriterStub]:
    channel = get_traced_grpc_channel(f"localhost:{nucliadb.ingest_grpc_port}", SERVICE_NAME)
    stub = WriterStub(channel)
    yield stub
    await channel.close(grace=None)


# alias to ease migration to new ndbfixtures
@pytest.fixture(scope="function")
async def standalone_nucliadb_grpc(standalone_nucliadb_ingest_grpc):
    yield standalone_nucliadb_ingest_grpc


# Utils


@pytest.fixture(scope="function")
async def ingest_grpc_server(
    maindb_driver: Driver,
    storage: Storage,
    shard_manager: KBShardManager,
) -> AsyncIterator[IngestGrpcServer]:
    """Ingest ORM gRPC server with dummy/mocked index."""
    servicer = WriterServicer()
    await servicer.initialize()
    server = get_traced_grpc_server(SERVICE_NAME)
    port = server.add_insecure_port("[::]:0")
    writer_pb2_grpc.add_WriterServicer_to_server(servicer, server)
    await server.start()
    yield IngestGrpcServer(
        host="127.0.0.1",
        port=port,
    )
    await servicer.finalize()
    await server.stop(None)
