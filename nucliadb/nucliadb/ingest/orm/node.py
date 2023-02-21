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
from enum import Enum
from typing import Dict, List, Optional
from uuid import uuid4

import prometheus_client  # type: ignore
from grpc import aio  # type: ignore
from lru import LRU  # type: ignore
from nucliadb_protos.nodereader_pb2_grpc import NodeReaderStub
from nucliadb_protos.noderesources_pb2 import EmptyQuery, ShardId
from nucliadb_protos.nodewriter_pb2_grpc import NodeSidecarStub, NodeWriterStub
from nucliadb_protos.writer_pb2 import ListMembersRequest, Member
from nucliadb_protos.writer_pb2 import ShardObject as PBShard
from nucliadb_protos.writer_pb2 import ShardReplica
from nucliadb_protos.writer_pb2 import Shards as PBShards
from nucliadb_protos.writer_pb2_grpc import WriterStub
from sentry_sdk import capture_exception

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
from nucliadb.sentry import SENTRY
from nucliadb_telemetry.grpc import OpenTelemetryGRPC
from nucliadb_telemetry.utils import get_telemetry
from nucliadb_utils.keys import KB_SHARDS

READ_CONNECTIONS = LRU(50)
WRITE_CONNECTIONS = LRU(50)
SIDECAR_CONNECTIONS = LRU(50)


AVAILABLE_NODES = prometheus_client.Gauge(
    "nucliadb_nodes_available",
    "Current number of nodes available",
)

SHARD_COUNT = prometheus_client.Gauge(
    "nucliadb_node_shard_count",
    "Current number of shards reported by nodes via chitchat",
    labelnames=["node"],
)

LOAD_SCORE = prometheus_client.Gauge(
    "nucliadb_node_load_score",
    "Current load score reported by nodes via chitchat",
    labelnames=["node"],
)


class NodeType(Enum):
    IO = 1
    SEARCH = 2
    INGEST = 3
    TRAIN = 4
    UNKNOWN = 5

    @staticmethod
    def from_str(label) -> NodeType:
        if label in "Io":
            return NodeType.IO
        elif label in "Search":
            return NodeType.SEARCH
        elif label in "Ingest":
            return NodeType.INGEST
        elif label in "Train":
            return NodeType.TRAIN
        else:
            logger.warning(f"Unknown '{label}' node type")
            return NodeType.UNKNOWN

    @staticmethod
    def from_pb(node_type: Member.Type.ValueType):
        if node_type == Member.Type.IO:
            return NodeType.IO
        elif node_type == Member.Type.SEARCH:
            return NodeType.SEARCH
        elif node_type == Member.Type.INGEST:
            return NodeType.INGEST
        elif node_type == Member.Type.TRAIN:
            return NodeType.TRAIN
        elif node_type == Member.Type.UNKNOWN:
            return NodeType.UNKNOWN
        else:
            raise ValueError(f"incompatible node type '{node_type}'")

    def to_pb(self) -> Member.Type.ValueType:
        if self == NodeType.IO:
            return Member.Type.IO
        elif self == NodeType.SEARCH:
            return Member.Type.SEARCH
        elif self == NodeType.INGEST:
            return Member.Type.INGEST
        elif self == NodeType.TRAIN:
            return Member.Type.TRAIN
        else:
            return Member.Type.UNKNOWN


@dataclass
class ClusterMember:
    node_id: str
    listen_addr: str
    type: NodeType
    online: bool
    is_self: bool
    load_score: float
    shard_count: int


class Node(AbstractNode):
    _writer: Optional[NodeWriterStub] = None
    _reader: Optional[NodeReaderStub] = None
    _sidecar: Optional[NodeSidecarStub] = None

    def __init__(
        self,
        address: str,
        type: NodeType,
        load_score: float,
        shard_count: int,
        dummy: bool = False,
    ):
        self.address = address
        self.type = type
        self.label = type.name
        self.load_score = load_score
        self.shard_count = shard_count
        self.dummy = dummy

    @classmethod
    def create_shard_klass(cls, shard_id: str, pbshard: PBShard):
        return Shard(sharduuid=shard_id, shard=pbshard)

    @classmethod
    async def create_shard_by_kbid(cls, txn: Transaction, kbid: str) -> Shard:
        kb_shards_key = KB_SHARDS.format(kbid=kbid)
        kb_shards: Optional[PBShards] = None
        kb_shards_binary = await txn.get(kb_shards_key)
        kb_nodes = []
        if kb_shards_binary:
            kb_shards = PBShards()
            kb_shards.ParseFromString(kb_shards_binary)
            # When adding a logic shard on an existing index, we need to
            # exclude nodes in which there is already a shard from the same KB
            kb_nodes = [
                replica.node for shard in kb_shards.shards for replica in shard.replicas
            ]

        try:
            node_ids = NODE_CLUSTER.find_nodes(exclude_nodes=kb_nodes)
        except NodeClusterSmall as err:
            if SENTRY:
                capture_exception(err)
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
                logger.info(
                    f"Node obj: {node} Shards: {node.shard_count} Load: {node.load_score}"
                )
                shard_created = await node.new_shard(kbid)
                replica = ShardReplica(node=str(node_id))
                replica.shard.CopyFrom(shard_created)
                shard.replicas.append(replica)
        except Exception as e:
            if SENTRY:
                capture_exception(e)
            logger.error("Error creating new shard")
            await cls.rollback_shard(shard)
            raise e

        if kb_shards is None:
            kb_shards = PBShards()
            kb_shards.kbid = kbid
            kb_shards.actual = -1

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
                    if SENTRY:
                        capture_exception(rollback_error)
                    logger.error(
                        f"New shard rollback error. Node: {node_id} Shard: {replica_id}"
                    )

    @classmethod
    async def create_shadow_shard(
        cls, txn: Transaction, kbid: str, node_id: str, replica_id: str
    ):
        try:
            shadow_created: Optional[ShardId] = None
            node = NODES.get(node_id)
            if node is None:
                raise ValueError(f"Node {node_id} not found")

            all_shards = await cls.get_all_shards(txn, kbid)
            if all_shards is None:
                raise KBNotFoundError()
            replica = get_replica(all_shards, replica_id)
            if replica is None:
                raise ReplicaShardNotFound()

            # Call sidecar's gRPC
            ssresp = await node.sidecar.CreateShadowShard(EmptyQuery())  # type: ignore
            if not ssresp.success:
                raise SidecarCreateShadowShardError("Sidecar error")
            shadow_created = ssresp.shard

            # Update shards object in maindb
            updated_shards = PBShards()
            updated_shards.CopyFrom(all_shards)

            updated: bool = update_shards_with_shadow_replica(
                updated_shards,
                replica_id,
                shadow_replica=ssresp.shard,
                shadow_node=node_id,
            )
            if not updated:
                raise UpdatingShardsError(f"Replica id not found: {replica_id}")
            key = KB_SHARDS.format(kbid=kbid)
            await txn.set(key, updated_shards.SerializeToString())
        except Exception as ex:
            logger.error(f"Error creating shadow shard. {ex}")
            if shadow_created:
                # Attempt to delete shadow shard
                try:
                    resp = await node.sidecar.DeleteShadowShard(shadow_created)  # type: ignore
                    assert resp.success
                except Exception:
                    logger.error(
                        f"Could not clean stale shadow shard: {shadow_created.id}"
                    )
            raise

    @classmethod
    async def delete_shadow_shard(cls, txn: Transaction, kbid: str, replica_id: str):
        # Check if requested replica has a shadow shard to delete
        all_shards = await cls.get_all_shards(txn, kbid)
        if all_shards is None:
            raise KBNotFoundError()
        replica = get_replica(all_shards, replica_id)
        if replica is None:
            raise ReplicaShardNotFound()
        if not replica.HasField("shadow_replica"):
            raise ShadowShardNotFound()

        # Shadow shard found. Delete it
        to_delete = replica.shadow_replica
        node = NODES.get(to_delete.node)
        if node is None:
            raise ValueError(f"Node {to_delete.node} not found")

        ssresp = await node.sidecar.DeleteShadowShard(to_delete.shard)  # type: ignore
        if not ssresp.success:
            raise SidecarDeleteShadowShardError("Sidecar error")

        # Now update the Shards pb object from maindb
        updated_shards = PBShards()
        updated_shards.CopyFrom(all_shards)
        cleaned = cleanup_shadow_replica_from_shards(
            updated_shards,
            replica_id,
        )
        if not cleaned:
            raise UpdatingShardsError()

        key = KB_SHARDS.format(kbid=kbid)
        await txn.set(key, updated_shards.SerializeToString())

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
        type: NodeType,
        load_score: float,
        shard_count: int,
        dummy: bool = False,
    ):
        NODES[ident] = Node(address, type, load_score, shard_count, dummy)

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
                member.listen_address,
                NodeType.from_pb(member.type),
                member.load_score,
                member.shard_count,
                member.dummy,
            )

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


async def chitchat_update_node(members: List[ClusterMember]) -> None:
    valid_ids = []
    for member in members:
        valid_ids.append(member.node_id)
        if member.is_self is False and member.type == NodeType.IO:
            node = NODES.get(member.node_id)
            if node is None:
                logger.debug(f"{member.node_id}/{member.type} add {member.listen_addr}")
                await Node.set(
                    member.node_id,
                    address=member.listen_addr,
                    type=member.type,
                    load_score=member.load_score,
                    shard_count=member.shard_count,
                )
                logger.debug("Node added")
            else:
                logger.debug(f"{member.node_id}/{member.type} update")
                node.load_score = member.load_score
                node.shard_count = member.shard_count
                logger.debug("Node updated")
    node_ids = [x for x in NODES.keys()]
    destroyed_node_ids = []
    for key in node_ids:
        if key not in valid_ids:
            node = NODES.get(key)
            if node is not None:
                destroyed_node_ids.append(key)
                logger.info(f"{key}/{node.type} remove {node.address}")
                await Node.destroy(key)
    update_node_metrics(NODES, destroyed_node_ids)


def update_shards_with_shadow_replica(
    shards: PBShards, replica_id: str, shadow_replica: ShardId, shadow_node: str
) -> bool:
    """
    Returns whether it found the replica to update in the shards proto message
    """
    replica = get_replica(shards, replica_id)
    if replica is None:
        return False
    replica.shadow_replica.node = shadow_node
    replica.shadow_replica.shard.CopyFrom(shadow_replica)
    return True


def cleanup_shadow_replica_from_shards(shards: PBShards, replica_id: str) -> bool:
    """
    Returns whether it found the replica to cleanup in the shards proto message
    """
    replica = get_replica(shards, replica_id)
    if replica is None:
        return False
    replica.ClearField("shadow_replica")
    return True


def get_replica(shards: PBShards, replica_id: str) -> Optional[ShardReplica]:
    for logic_shard in shards.shards:
        for replica_shard in logic_shard.replicas:
            if replica_shard.shard.id == replica_id:
                return replica_shard
    return None


class SidecarCreateShadowShardError(Exception):
    pass


class SidecarDeleteShadowShardError(Exception):
    pass


class UpdatingShardsError(Exception):
    pass


class ShadowShardNotFound(Exception):
    pass


class ReplicaShardNotFound(Exception):
    pass


class KBNotFoundError(Exception):
    pass


def update_node_metrics(nodes: Dict[str, Node], destroyed_node_ids: List[str]):
    AVAILABLE_NODES.set(len(nodes))

    for node_id, node in nodes.items():
        SHARD_COUNT.labels(node=node_id).set(node.shard_count)
        LOAD_SCORE.labels(node=node_id).set(node.load_score)

    for node_id in destroyed_node_ids:
        for gauge in (SHARD_COUNT, LOAD_SCORE):
            try:
                gauge.remove(node_id)
            except KeyError:
                # Be resilient if there were no previous
                # samples for this node_id
                pass
