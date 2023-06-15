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

from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.nodesidecar_pb2_grpc import NodeSidecarStub
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub

from nucliadb.common.cluster.abc import AbstractIndexNode  # type: ignore
from nucliadb.common.cluster.grpc_node_dummy import (  # type: ignore
    DummyReaderStub,
    DummySidecarStub,
    DummyWriterStub,
)
from nucliadb.ingest import SERVICE_NAME
from nucliadb_utils.grpc import get_traced_grpc_channel

from .settings import settings

READ_CONNECTIONS = LRU(50)
WRITE_CONNECTIONS = LRU(50)
SIDECAR_CONNECTIONS = LRU(50)


class IndexNode(AbstractIndexNode):
    _writer: Optional[NodeWriterStub] = None
    _reader: Optional[NodeReaderStub] = None
    _sidecar: Optional[NodeSidecarStub] = None

    def _get_service_address(
        self, port_map: dict[str, int], port: Optional[int]
    ) -> str:
        hostname = self.address.split(":")[0]
        if port is None:
            # For testing purposes we need to be able to have a writing port
            port = port_map[hostname]
            grpc_address = f"localhost:{port}"
        else:
            grpc_address = f"{hostname}:{port}"
        return grpc_address

    @property
    def sidecar(self) -> NodeSidecarStub:
        if self._sidecar is None and self.address not in SIDECAR_CONNECTIONS:
            if not self.dummy:
                grpc_address = self._get_service_address(
                    settings.sidecar_port_map, settings.node_sidecar_port
                )
                channel = get_traced_grpc_channel(
                    grpc_address, SERVICE_NAME, variant="_sidecar"
                )
                SIDECAR_CONNECTIONS[self.address] = NodeSidecarStub(channel)
            else:
                SIDECAR_CONNECTIONS[self.address] = DummySidecarStub()
        if self._sidecar is None:
            self._sidecar = SIDECAR_CONNECTIONS[self.address]
        return self._sidecar

    @property
    def writer(self) -> NodeWriterStub:
        if self._writer is None and self.address not in WRITE_CONNECTIONS:
            if not self.dummy:
                grpc_address = self._get_service_address(
                    settings.writer_port_map, settings.node_writer_port
                )
                channel = get_traced_grpc_channel(
                    grpc_address, SERVICE_NAME, variant="_writer"
                )
                WRITE_CONNECTIONS[self.address] = NodeWriterStub(channel)
            else:
                WRITE_CONNECTIONS[self.address] = DummyWriterStub()
        if self._writer is None:
            self._writer = WRITE_CONNECTIONS[self.address]
        return self._writer

    @property
    def reader(self) -> NodeReaderStub:
        if self._reader is None and self.address not in READ_CONNECTIONS:
            if not self.dummy:
                grpc_address = self._get_service_address(
                    settings.reader_port_map, settings.node_reader_port
                )
                channel = get_traced_grpc_channel(
                    grpc_address, SERVICE_NAME, variant="_reader"
                )
                READ_CONNECTIONS[self.address] = NodeReaderStub(channel)
            else:
                READ_CONNECTIONS[self.address] = DummyReaderStub()
        if self._reader is None:
            self._reader = READ_CONNECTIONS[self.address]
        return self._reader
