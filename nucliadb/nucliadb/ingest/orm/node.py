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

from typing import Dict, List, Optional
from uuid import uuid4

from grpc import aio  # type: ignore
from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.nodesidecar_pb2_grpc import NodeSidecarStub
from nucliadb_protos.nodewriter_pb2_grpc import NodeWriterStub
from nucliadb_protos.utils_pb2 import VectorSimilarity
from nucliadb_protos.writer_pb2 import ListMembersRequest, Member
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards
from nucliadb_protos.writer_pb2_grpc import WriterStub

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.maindb.driver import Transaction
from nucliadb.ingest.orm import NODE_CLUSTER, NODES, NodeClusterSmall
from nucliadb.ingest.orm.abc import AbstractNode  # type: ignore
from nucliadb.ingest.orm.exceptions import NodesUnsync  # type: ignore
from nucliadb.ingest.orm.grpc_node_dummy import (  # type: ignore
    DummyReaderStub,
    DummySidecarStub,
    DummyWriterStub,
)
from nucliadb.ingest.orm.shard import Shard
from nucliadb.ingest.settings import settings
from nucliadb_models.cluster import MemberType
from nucliadb_telemetry import errors
from nucliadb_utils.grpc import get_traced_grpc_channel
from nucliadb_utils.keys import KB_SHARDS

READ_CONNECTIONS = LRU(50)
WRITE_CONNECTIONS = LRU(50)
SIDECAR_CONNECTIONS = LRU(50)


class Node(AbstractNode):
    _writer: Optional[NodeWriterStub] = None
    _reader: Optional[NodeReaderStub] = None
    _sidecar: Optional[NodeSidecarStub] = None

    def __init__(
        self,
        *,
        id: str,
        address: str,
        type: MemberType,
        shard_count: int,
        dummy: bool = False,
    ):
        self.id = id
        self.address = address
        self.type = type
        self.label = type.name
        self.shard_count = shard_count
        self.dummy = dummy

    def __str__(self):
        return f"{self.__class__.__name__}({self.id}, {self.address})"

    def __repr__(self):
        return self.__str__()

    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard):
        return Shard(sharduuid=shard_id, shard=pbshard)

    @classmethod
    async def create_shard_by_kbid(
        cls,
        txn: Transaction,
        kbid: str,
        similarity: VectorSimilarity.ValueType = VectorSimilarity.COSINE,
    ) -> Shard:
        kb_shards_key = KB_SHARDS.format(kbid=kbid)
        kb_shards: Optional[PBShards] = None
        kb_shards_binary = await txn.get(kb_shards_key)
        if not kb_shards_binary:
            # First logic shard on the index
            kb_shards = PBShards()
            kb_shards.kbid = kbid
            kb_shards.actual = -1
            kb_shards.similarity = similarity
        else:
            # New logic shard on an existing index
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_binary)

        try:
            existing_kb_nodes = [
                replica.node for shard in kb_shards.shards for replica in shard.replicas
            ]
            node_ids = NODE_CLUSTER.find_nodes(avoid_nodes=existing_kb_nodes)
        except NodeClusterSmall as err:
            errors.capture_exception(err)
            logger.error(
                f"Shard creation for kbid={kbid} failed: Replication requirements could not be met."
            )
            raise

        sharduuid = uuid4().hex
        shard = PBShard(shard=sharduuid)
        try:
            for node_id in node_ids:
                logger.info(f"Node description: {node_id}")
                node = NODES.get(node_id)
                if node is None:
                    raise NodesUnsync(f"Node {node_id} is not found or not available")
                logger.info(f"Node obj: {node} Shards: {node.shard_count}")
                shard_created = await node.new_shard(
                    kbid, similarity=kb_shards.similarity
                )
                replica = ShardReplica(node=str(node_id))
                replica.shard.CopyFrom(shard_created)
                shard.replicas.append(replica)
        except Exception as e:
            errors.capture_exception(e)
            logger.error("Error creating new shard")
            await cls.rollback_shard(shard)
            raise e

        # Append the created shard and make `actual` point to it.
        kb_shards.shards.append(shard)
        kb_shards.actual += 1

        await txn.set(kb_shards_key, kb_shards.SerializeToString())

        return Shard(sharduuid=sharduuid, shard=shard)

    @classmethod
    async def rollback_shard(cls, shard: PBShard):
        for shard_replica in shard.replicas:
            node_id = shard_replica.node
            replica_id = shard_replica.shard.id
            node = NODES.get(node_id)
            if node is not None:
                try:
                    await node.delete_shard(replica_id)
                except Exception as rollback_error:
                    errors.capture_exception(rollback_error)
                    logger.error(
                        f"New shard rollback error. Node: {node_id} Shard: {replica_id}"
                    )

    @classmethod
    async def get_current_active_shard(
        cls, txn: Transaction, kbid: str
    ) -> Optional[Shard]:
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
        type: MemberType,
        shard_count: int,
        dummy: bool = False,
    ):
        NODES[ident] = Node(
            id=ident, address=address, type=type, shard_count=shard_count, dummy=dummy
        )

    @classmethod
    async def get(cls, ident: str) -> Optional[Node]:
        return NODES.get(ident)

    @classmethod
    async def destroy(cls, ident: str):
        del NODES[ident]

    @classmethod
    async def load_active_nodes(cls):
        from nucliadb_utils.settings import nucliadb_settings

        stub = WriterStub(aio.insecure_channel(nucliadb_settings.nucliadb_ingest))
        request = ListMembersRequest()
        members = await stub.ListMembers(request)
        for member in members.members:
            NODES[member.id] = Node(
                id=member.id,
                address=member.listen_address,
                type=MemberType.from_pb(member.type),
                shard_count=member.shard_count,
                dummy=member.dummy,
            )

    @classmethod
    async def list_members(cls) -> List[Member]:
        members = []
        for nodeid, node in NODES.items():
            member = Member(
                id=str(nodeid),
                listen_address=node.address,
                type=node.type.to_pb(),
                shard_count=node.shard_count,
                dummy=node.dummy,
            )
            members.append(member)
        return members

    def _get_service_address(
        self, port_map: Dict[str, int], port: Optional[int]
    ) -> str:
        hostname = self.address.split(":")[0]
        if port is None:
            # For testing proposes we need to be able to have a writing port
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
