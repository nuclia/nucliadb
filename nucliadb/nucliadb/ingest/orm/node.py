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
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional
from uuid import uuid4

from grpc import aio  # type: ignore
from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.noderesources_pb2 import EmptyQuery
from nucliadb_protos.noderesources_pb2 import Shard as NodeResourcesShard
from nucliadb_protos.noderesources_pb2 import ShardCreated, ShardId, ShardList
from nucliadb_protos.nodewriter_pb2 import OpStatus
from nucliadb_protos.nodewriter_pb2_grpc import NodeSidecarStub, NodeWriterStub
from nucliadb_protos.writer_pb2 import ListMembersRequest
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm import NODE_CLUSTER, NODES
from nucliadb.ingest.orm.abc import AbstractNode  # type: ignore
from nucliadb.ingest.orm.exceptions import NodesUnsync  # type: ignore
from nucliadb.ingest.orm.shard import Shard
from nucliadb.ingest.settings import settings
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.keys import KB_SHARDS

READ_CONNECTIONS = LRU(50)
WRITE_CONNECTIONS = LRU(50)
SIDECAR_CONNECTIONS = LRU(50)


class DummyWriterStub:
    calls: Dict[str, List[Any]] = {}

    async def GetShard(self, data):
        self.calls.setdefault("GetShard", []).append(data)
        return Shard(shard_id="shard", resources=2)

    async def NewShard(self, data):
        self.calls.setdefault("NewShard", []).append(data)
        return ShardCreated(id="shard")

    async def DeleteShard(self, data):
        self.calls.setdefault("DeleteShard", []).append(data)
        return ShardId(id="shard")

    async def ListShards(self, data):
        self.calls.setdefault("ListShards", []).append(data)
        sl = ShardList()
        sl.shards.append(NodeResourcesShard(shard_id="shard", resources=2))
        sl.shards.append(NodeResourcesShard(shard_id="shard2", resources=4))
        return sl

    async def SetResource(self, data):
        self.calls.setdefault("SetResource", []).append(data)
        result = OpStatus()
        result.count = 1
        return result


class DummyReaderStub:
    calls: Dict[str, List[Any]] = {}

    async def GetShard(self, data):
        self.calls.setdefault("GetShard", []).append(data)
        return Shard(shard_id="shard", resources=2)


class DummySidecarStub:
    calls: Dict[str, List[Any]] = {}

    async def GetCount(self, data):
        self.calls.setdefault("GetCount", []).append(data)
        return Shard(shard_id="shard", resources=2)


@dataclass
class ClusterMember:
    node_id: str
    listen_addr: str
    node_type: str
    online: bool
    is_self: bool


class Node(AbstractNode):
    _writer: Optional[NodeWriterStub] = None
    _reader: Optional[NodeReaderStub] = None
    _sidecar: Optional[NodeSidecarStub] = None

    def __init__(self, address: str, label: str, dummy: bool = False):
        self.address = address
        self.label = label
        self.dummy = dummy

    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard):
        return Shard(sharduuid=shard_id, shard=pbshard)

    @classmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> Shard:
        nodes = NODE_CLUSTER.find_nodes(kbid)
        sharduuid = uuid4().hex
        shard = PBShard(shard=sharduuid)
        try:
            for node in nodes:
                print(f"Node description: {node}")
                node_obj = NODES.get(node)
                print(f"Node obj: {node_obj}")
                if node_obj is None:
                    raise NodesUnsync()
                shard_created = await node_obj.new_shard()
                sr = ShardReplica(node=str(node))
                sr.shard.CopyFrom(shard_created)
                shard.replicas.append(sr)
        except Exception as e:
            # rollback
            for shard_replica in shard.replicas:
                node = NODES.get(shard_replica.node)
                if node is not None:
                    await node.delete_shard(shard_replica.shard.id)
            raise e

        key = KB_SHARDS.format(kbid=kbid)
        payload = await txn.get(key)
        kb_shards = PBShards()
        if payload is not None:
            kb_shards.ParseFromString(payload)
        else:
            kb_shards.kbid = kbid
            kb_shards.actual = -1
        kb_shards.shards.append(shard)
        kb_shards.actual += 1
        await txn.set(key, kb_shards.SerializeToString())

        return Shard(sharduuid=sharduuid, shard=shard)

    @classmethod
    async def actual_shard(cls, txn: Transaction, kbid: str) -> Optional[Shard]:
        key = KB_SHARDS.format(kbid=kbid)
        kb_shards_bytes: Optional[bytes] = await txn.get(key)
        if kb_shards_bytes is not None:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_bytes)
            shard: PBShard = kb_shards.shards[kb_shards.actual]
            return Shard(sharduuid=shard.shard, shard=shard)
        else:
            return None

    @classmethod
    async def set(
        cls,
        ident: str,
        address: str,
        label: str,
        dummy: bool = False,
    ):
        NODES[ident] = Node(address, label, dummy)
        # Compute cluster
        NODE_CLUSTER.compute()

    @classmethod
    async def get(cls, ident: str) -> Optional[Node]:
        return NODES.get(ident)

    @classmethod
    async def destroy(cls, ident: str):
        del NODES[ident]
        NODE_CLUSTER.compute()

    @classmethod
    async def load_active_nodes(cls):
        from nucliadb_utils.settings import nucliadb_settings

        stub = WriterStub(aio.insecure_channel(nucliadb_settings.nucliadb_ingest))
        request = ListMembersRequest()
        members = await stub.ListMembers(request)
        for member in members.members:
            NODES[member.id] = Node(member.listen_address, member.type, member.dummy)

    @property
    def sidecar(self) -> NodeSidecarStub:
        if (
            self._sidecar is None
            and self.address not in SIDECAR_CONNECTIONS
            and self.dummy is False
        ):
            hostname = self.address.split(":")[0]
            if settings.node_sidecar_port is None:
                # For testing proposes we need to be able to have a writing port
                sidecar_port = settings.sidecar_port_map[hostname]
                grpc_address = f"localhost:{sidecar_port}"
            else:
                grpc_address = f"{hostname}:{settings.node_sidecar_port}"

            tracer_provider = get_telemetry(SERVICE_NAME)
            if tracer_provider is not None:
                telemetry_grpc = OpenTelemetryGRPC(
                    f"{SERVICE_NAME}_grpc_sidecar", tracer_provider
                )
                channel = telemetry_grpc.init_client(grpc_address)
            else:
                channel = aio.insecure_channel(grpc_address)

            SIDECAR_CONNECTIONS[self.address] = NodeSidecarStub(channel)
        if (
            self._sidecar is None
            and self.address not in SIDECAR_CONNECTIONS
            and self.dummy is True
        ):
            SIDECAR_CONNECTIONS[self.address] = DummySidecarStub()
        if self._sidecar is None:
            self._sidecar = SIDECAR_CONNECTIONS[self.address]
        return self._sidecar

    @property
    def writer(self) -> NodeWriterStub:
        if (
            self._writer is None
            and self.address not in WRITE_CONNECTIONS
            and self.dummy is False
        ):
            hostname = self.address.split(":")[0]
            if settings.node_writer_port is None:
                # For testing proposes we need to be able to have a writing port
                writer_port = settings.writer_port_map[hostname]
                grpc_address = f"localhost:{writer_port}"
            else:
                grpc_address = f"{hostname}:{settings.node_writer_port}"

            tracer_provider = get_telemetry(SERVICE_NAME)
            if tracer_provider is not None:
                telemetry_grpc = OpenTelemetryGRPC(
                    f"{SERVICE_NAME}_grpc_writer", tracer_provider
                )
                channel = telemetry_grpc.init_client(grpc_address)
            else:
                channel = aio.insecure_channel(grpc_address)

            WRITE_CONNECTIONS[self.address] = NodeWriterStub(channel)
        if (
            self._writer is None
            and self.address not in WRITE_CONNECTIONS
            and self.dummy is True
        ):
            WRITE_CONNECTIONS[self.address] = DummyWriterStub()
        if self._writer is None:
            self._writer = WRITE_CONNECTIONS[self.address]
        return self._writer

    @property
    def reader(self) -> NodeReaderStub:
        if (
            self._reader is None
            and self.address not in READ_CONNECTIONS
            and self.dummy is False
        ):
            hostname = self.address.split(":")[0]
            if settings.node_reader_port is None:
                # For testing proposes we need to be able to have a writing port
                reader_port = settings.reader_port_map[hostname]
                grpc_address = f"localhost:{reader_port}"
            else:
                grpc_address = f"{hostname}:{settings.node_reader_port}"

            tracer_provider = get_telemetry(SERVICE_NAME)
            if tracer_provider is not None:
                telemetry_grpc = OpenTelemetryGRPC(
                    f"{SERVICE_NAME}_grpc_reader", tracer_provider
                )
                channel = telemetry_grpc.init_client(grpc_address)
            else:
                channel = aio.insecure_channel(grpc_address)

            READ_CONNECTIONS[self.address] = NodeReaderStub(channel)
        if (
            self._reader is None
            and self.address not in READ_CONNECTIONS
            and self.dummy is True
        ):
            READ_CONNECTIONS[self.address] = DummyReaderStub()
        if self._reader is None:
            self._reader = READ_CONNECTIONS[self.address]
        return self._reader

    async def get_shard(self, id: str) -> ShardId:
        req = ShardId(id=id)
        resp = await self.writer.GetShard(req)  # type: ignore
        return resp

    async def get_reader_shard(self, id: str) -> NodeResourcesShard:
        req = ShardId(id=id)
        resp = await self.reader.GetShard(req)  # type: ignore
        return resp

    async def new_shard(self) -> ShardCreated:
        req = EmptyQuery()
        resp = await self.writer.NewShard(req)  # type: ignore
        return resp

    async def delete_shard(self, id: str) -> int:
        req = ShardId(id=id)
        resp = await self.writer.DeleteShard(req)  # type: ignore
        return resp.id

    async def list_shards(self) -> List[str]:
        req = EmptyQuery()
        resp = await self.writer.ListShards(req)  # type: ignore
        return resp.shards


async def chitchat_update_node(members: List[ClusterMember]) -> None:
    valid_ids = []
    for member in members:
        valid_ids.append(member.node_id)
        if (
            member.is_self is False
            and member.node_type == "Node"
            and member.node_id not in NODES
        ):
            print(
                f"logger debug: {member.node_id}/{member.node_type} add {member.listen_addr}"
            )
            logger.debug(
                f"{member.node_id}/{member.node_type} add {member.listen_addr}"
            )
            await Node.set(
                member.node_id,
                address=member.listen_addr,
                label=member.node_type,
            )
            print("Node added")
    node_ids = [x for x in NODES.keys()]
    for key in node_ids:
        if key not in valid_ids:
            node = NODES.get(key)
            if node is not None:
                logger.info(f"{key}/{node.label} remove {node.address}")
                await Node.destroy(key)


class DefinedNodesNucliaDBSearch:
    async def start(self):
        await Node.load_active_nodes()
