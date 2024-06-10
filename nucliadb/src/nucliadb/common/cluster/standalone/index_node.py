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
from typing import Any, Optional

from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.grpc_node_dummy import DummyReaderStub, DummyWriterStub
from nucliadb.common.cluster.settings import settings as cluster_settings
from nucliadb.common.cluster.standalone import grpc_node_binding
from nucliadb_protos import standalone_pb2, standalone_pb2_grpc
from nucliadb_utils.grpc import get_traced_grpc_channel


class StandaloneIndexNode(AbstractIndexNode):
    _writer: grpc_node_binding.StandaloneWriterWrapper
    _reader: grpc_node_binding.StandaloneReaderWrapper
    label: str = "standalone"

    def __init__(
        self,
        id: str,
        address: str,
        shard_count: int,
        available_disk: int,
        dummy: bool = False,
        primary_id: Optional[str] = None,
    ):
        super().__init__(
            id=id,
            address=address,
            shard_count=shard_count,
            available_disk=available_disk,
            dummy=dummy,
            # standalone does not support read replicas
            primary_id=None,
        )
        if dummy:
            self._writer = DummyWriterStub()  # type: ignore
            self._reader = DummyReaderStub()  # type: ignore
        else:
            self._writer = grpc_node_binding.StandaloneWriterWrapper()
            self._reader = grpc_node_binding.StandaloneReaderWrapper()

    @property
    def reader(self) -> grpc_node_binding.StandaloneReaderWrapper:  # type: ignore
        return self._reader

    @property
    def writer(self) -> grpc_node_binding.StandaloneWriterWrapper:  # type: ignore
        return self._writer


class ProxyCallerWrapper:
    def __init__(self, address: str, type: str, original_type: Any):
        self._address = address
        self._type = type
        self._original_type = original_type
        if ":" not in address:
            grpc_address = f"{address}:{cluster_settings.standalone_node_port}"
        else:
            grpc_address = address
        self._channel = get_traced_grpc_channel(grpc_address, "standalone_proxy")
        self._stub = standalone_pb2_grpc.StandaloneClusterServiceStub(self._channel)

    def __getattr__(self, name):
        async def call(request):
            req = standalone_pb2.NodeActionRequest(
                service=self._type, action=name, payload=request.SerializeToString()
            )
            resp = await self._stub.NodeAction(req)
            try:
                if self._type == "reader":
                    _, return_type = grpc_node_binding.READER_METHODS[name]
                elif self._type == "writer":
                    _, return_type = grpc_node_binding.WRITER_METHODS[name]
                else:
                    raise NotImplementedError(f"Unknown type {self._type}")
            except KeyError:
                raise NotImplementedError(f"Unknown method for type {self._type}: {name}")
            return_value = return_type()
            return_value.ParseFromString(resp.payload)
            return return_value

        return call


class ProxyStandaloneIndexNode(StandaloneIndexNode):
    label: str = "proxy_standalone"

    def __init__(
        self,
        id: str,
        address: str,
        shard_count: int,
        available_disk: int,
        dummy: bool = False,
    ):
        super().__init__(id, address, shard_count, available_disk=available_disk, dummy=dummy)
        if dummy:
            return

        self._writer = ProxyCallerWrapper(  # type: ignore
            address, "writer", grpc_node_binding.StandaloneWriterWrapper
        )
        self._reader = ProxyCallerWrapper(  # type: ignore
            address, "reader", grpc_node_binding.StandaloneReaderWrapper
        )
