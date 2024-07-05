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
import logging
import uuid
from typing import Any, Awaitable, Callable, Optional

import backoff

from nucliadb.common import datamanagers
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import (
    ExhaustedNodesError,
    NodeClusterSmall,
    NodeError,
    NodesUnsync,
    NoHealthyNodeAvailable,
    ShardNotFound,
    ShardsNotFound,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import (
    knowledgebox_pb2,
    nodereader_pb2,
    noderesources_pb2,
    nodewriter_pb2,
    writer_pb2,
)
from nucliadb_protos.nodewriter_pb2 import IndexMessage, IndexMessageSource, TypeMessage
from nucliadb_telemetry import errors
from nucliadb_utils.utilities import get_indexing, get_storage

from .index_node import IndexNode
from .settings import settings
from .standalone.index_node import ProxyStandaloneIndexNode
from .standalone.utils import get_self, get_standalone_node_id, is_index_node

logger = logging.getLogger(__name__)

INDEX_NODES: dict[str, AbstractIndexNode] = {}
READ_REPLICA_INDEX_NODES: dict[str, set[str]] = {}


def get_index_nodes(include_secondary: bool = False) -> list[AbstractIndexNode]:
    all_nodes = [inode for inode in INDEX_NODES.values()]
    if not include_secondary:
        return [inode for inode in all_nodes if inode.primary_id is None]
    return all_nodes


def get_index_node(node_id: str) -> Optional[AbstractIndexNode]:
    return INDEX_NODES.get(node_id)


def clear_index_nodes():
    INDEX_NODES.clear()
    READ_REPLICA_INDEX_NODES.clear()


def get_read_replica_node_ids(node_id: str) -> list[str]:
    return list(READ_REPLICA_INDEX_NODES.get(node_id, set()))


def add_index_node(
    *,
    id: str,
    address: str,
    shard_count: int,
    available_disk: int,
    dummy: bool = False,
    primary_id: Optional[str] = None,
) -> AbstractIndexNode:
    if settings.standalone_mode:
        if is_index_node() and id == get_standalone_node_id():
            node = get_self()
        else:
            node = ProxyStandaloneIndexNode(
                id=id,
                address=address,
                shard_count=shard_count,
                available_disk=available_disk,
                dummy=dummy,
            )
    else:
        node = IndexNode(  # type: ignore
            id=id,
            address=address,
            shard_count=shard_count,
            available_disk=available_disk,
            dummy=dummy,
            primary_id=primary_id,
        )
    INDEX_NODES[id] = node
    if primary_id is not None:
        if primary_id not in READ_REPLICA_INDEX_NODES:
            READ_REPLICA_INDEX_NODES[primary_id] = set()
        READ_REPLICA_INDEX_NODES[primary_id].add(id)
    return node


def remove_index_node(node_id: str, primary_id: Optional[str] = None) -> None:
    INDEX_NODES.pop(node_id, None)
    if primary_id is not None and primary_id in READ_REPLICA_INDEX_NODES:
        if node_id in READ_REPLICA_INDEX_NODES[primary_id]:
            READ_REPLICA_INDEX_NODES[primary_id].remove(node_id)


class KBShardManager:
    # TODO: move to data manager
    async def get_shards_by_kbid_inner(self, kbid: str) -> writer_pb2.Shards:
        async with datamanagers.with_ro_transaction() as txn:
            result = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
            if result is None:
                # could be None because /shards doesn't exist, or beacause the
                # whole KB does not exist. In any case, this should not happen
                raise ShardsNotFound(kbid)
            return result

    # TODO: move to data manager
    async def get_shards_by_kbid(self, kbid: str) -> list[writer_pb2.ShardObject]:
        shards = await self.get_shards_by_kbid_inner(kbid)
        return [x for x in shards.shards]

    async def apply_for_all_shards(
        self,
        kbid: str,
        aw: Callable[[AbstractIndexNode, str], Awaitable[Any]],
        timeout: float,
        use_read_replica_nodes: bool = False,
    ) -> list[Any]:
        shards = await self.get_shards_by_kbid(kbid)
        ops = []

        for shard_obj in shards:
            node, shard_id = choose_node(shard_obj, use_read_replica_nodes=use_read_replica_nodes)
            if shard_id is None:
                raise ShardNotFound("Found a node but not a shard")

            ops.append(aw(node, shard_id))

        try:
            results = await asyncio.wait_for(
                asyncio.gather(*ops, return_exceptions=True),
                timeout=timeout,
            )
        except asyncio.TimeoutError as exc:
            errors.capture_exception(exc)
            raise NodeError("Node unavailable for operation") from exc

        return results

    # TODO: move to data manager
    async def get_current_active_shard(
        self, txn: Transaction, kbid: str
    ) -> Optional[writer_pb2.ShardObject]:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=False)
        if kb_shards is None:
            return None

        # B/c with Shards.actual
        # Just ignore the new attribute for now
        shard = kb_shards.shards[kb_shards.actual]
        return shard

    # TODO: logic about creation and read-only shards should be decoupled
    async def create_shard_by_kbid(
        self,
        txn: Transaction,
        kbid: str,
    ) -> writer_pb2.ShardObject:
        try:
            check_enough_nodes()
        except NodeClusterSmall as err:
            errors.capture_exception(err)
            logger.error(
                f"Shard creation for kbid={kbid} failed: Replication requirements could not be met."
            )
            raise

        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
        if kb_shards is None:
            msg = ("Attempting to create a shard for a KB when it has no stored shards in maindb",)
            logger.error(msg, extra={"kbid": kbid})
            raise ShardsNotFound(msg)

        existing_kb_nodes = [replica.node for shard in kb_shards.shards for replica in shard.replicas]
        nodes = sorted_primary_nodes(
            avoid_nodes=existing_kb_nodes,
            ignore_nodes=settings.drain_nodes,
        )

        shard_uuid = uuid.uuid4().hex
        shard = writer_pb2.ShardObject(shard=shard_uuid, read_only=False)
        try:
            # Attempt to create configured number of replicas
            replicas_created = 0
            while replicas_created < settings.node_replicas:
                try:
                    node_id = nodes.pop(0)
                except IndexError:
                    # It was not possible to find enough nodes
                    # available/responsive to create the required replicas
                    raise ExhaustedNodesError()

                node = get_index_node(node_id)
                if node is None:
                    logger.error(f"Node {node_id} is not found or not available")
                    continue
                is_matryoshka = len(kb_shards.model.matryoshka_dimensions) > 0
                vector_index_config = nodewriter_pb2.VectorIndexConfig(
                    similarity=kb_shards.similarity,
                    vector_type=nodewriter_pb2.VectorType.DENSE_F32,
                    vector_dimension=kb_shards.model.vector_dimension,
                    normalize_vectors=is_matryoshka,
                )
                try:
                    shard_created = await node.new_shard(
                        kbid,
                        release_channel=kb_shards.release_channel,
                        vector_index_config=vector_index_config,
                    )
                except Exception as e:
                    errors.capture_exception(e)
                    logger.exception(f"Error creating new shard at {node}: {e}")
                    continue

                shard_id = shard_created.id
                try:
                    async for vectorset_config in datamanagers.vectorsets.iter(txn, kbid=kbid):
                        response = await node.add_vectorset(
                            shard_id,
                            vectorset=vectorset_config.vectorset_id,
                            config=vectorset_config.vectorset_index_config,
                        )
                        if response.status != response.Status.OK:
                            raise Exception(response.detail)
                except Exception as e:
                    errors.capture_exception(e)
                    logger.exception(
                        "Error creating vectorset '{vectorset_id}' new shard at {node}: {details}".format(
                            vectorset_id=vectorset_config.vectorset_id,
                            node=node,
                            details=e,
                        )
                    )
                    continue

                replica = writer_pb2.ShardReplica(node=str(node_id))
                replica.shard.CopyFrom(shard_created)
                shard.replicas.append(replica)
                replicas_created += 1
        except Exception as e:
            errors.capture_exception(e)
            logger.error(f"Unexpected error creating new shard: {e}")
            await self.rollback_shard(shard)
            raise e

        # set previous shard as read only, we only have one writable shard at a
        # time
        if len(kb_shards.shards) > 0:
            kb_shards.shards[-1].read_only = True

        # Append the created shard and make `actual` point to it.
        kb_shards.shards.append(shard)
        # B/c with Shards.actual - we only use last created shard
        kb_shards.actual = len(kb_shards.shards) - 1

        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)

        return shard

    async def rollback_shard(self, shard: writer_pb2.ShardObject):
        for shard_replica in shard.replicas:
            node_id = shard_replica.node
            replica_id = shard_replica.shard.id
            node = get_index_node(node_id)
            if node is not None:
                try:
                    logger.info(
                        "Deleting shard replica",
                        extra={"shard": replica_id, "node": node_id},
                    )
                    await node.delete_shard(replica_id)
                except Exception as rollback_error:
                    errors.capture_exception(rollback_error)
                    logger.error(
                        f"New shard rollback error. Node: {node_id} Shard: {replica_id}",
                        exc_info=True,
                    )

    def indexing_replicas(self, shard: writer_pb2.ShardObject) -> list[tuple[str, str]]:
        """
        Returns the replica ids and nodes for the shard replicas
        """
        result = []
        for replica in shard.replicas:
            result.append((replica.shard.id, replica.node))
        return result

    async def delete_resource(
        self,
        shard: writer_pb2.ShardObject,
        uuid: str,
        txid: int,
        partition: str,
        kb: str,
    ) -> None:
        indexing = get_indexing()
        storage = await get_storage()

        await storage.delete_indexing(resource_uid=uuid, txid=txid, kb=kb, logical_shard=shard.shard)

        for replica_id, node_id in self.indexing_replicas(shard):
            indexpb: nodewriter_pb2.IndexMessage = nodewriter_pb2.IndexMessage()
            indexpb.node = node_id
            indexpb.shard = replica_id
            indexpb.txid = txid
            indexpb.resource = uuid
            indexpb.typemessage = nodewriter_pb2.TypeMessage.DELETION
            indexpb.partition = partition
            indexpb.kbid = kb
            await indexing.index(indexpb, node_id)

    async def add_resource(
        self,
        shard: writer_pb2.ShardObject,
        resource: noderesources_pb2.Resource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
        source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
    ) -> None:
        """
        Stores the Resource object in the object storage and sends an IndexMessage to the indexing Nats stream.
        """
        if txid == -1 and reindex_id is None:
            # This means we are injecting a complete resource via ingest gRPC
            # outside of a transaction. We need to treat this as a reindex operation.
            reindex_id = uuid.uuid4().hex

        storage = await get_storage()
        indexing = get_indexing()

        indexpb = IndexMessage()

        if reindex_id is not None:
            storage_key = await storage.reindexing(
                resource, reindex_id, partition, kb=kb, logical_shard=shard.shard
            )
            indexpb.reindex_id = reindex_id
        else:
            storage_key = await storage.indexing(
                resource, txid, partition, kb=kb, logical_shard=shard.shard
            )
            indexpb.txid = txid

        indexpb.typemessage = TypeMessage.CREATION
        indexpb.storage_key = storage_key
        indexpb.kbid = kb
        if partition:
            indexpb.partition = partition
        indexpb.source = source
        indexpb.resource = resource.resource.uuid

        for replica_id, node_id in self.indexing_replicas(shard):
            indexpb.node = node_id
            indexpb.shard = replica_id
            await indexing.index(indexpb, node_id)

    def should_create_new_shard(self, num_paragraphs: int) -> bool:
        return num_paragraphs > settings.max_shard_paragraphs

    async def maybe_create_new_shard(
        self,
        kbid: str,
        num_paragraphs: int,
    ):
        if not self.should_create_new_shard(num_paragraphs):
            return

        logger.info({"message": "Adding shard", "kbid": kbid})

        async with datamanagers.with_transaction() as txn:
            await self.create_shard_by_kbid(txn, kbid)
            await txn.commit()

    async def create_vectorset(self, kbid: str, config: knowledgebox_pb2.VectorSetConfig):
        """Create a new vectorset in all KB shards."""

        async def _create_vectorset(node: AbstractIndexNode, shard_id: str):
            vectorset_id = config.vectorset_id
            index_config = config.vectorset_index_config
            result = await node.add_vectorset(shard_id, vectorset_id, index_config)
            if result.status != result.Status.OK:
                raise NodeError(
                    f"Unable to create vectorset {vectorset_id} in kb {kbid} shard {shard_id}"
                )

        await self.apply_for_all_shards(
            kbid, _create_vectorset, timeout=10, use_read_replica_nodes=False
        )

    async def delete_vectorset(self, kbid: str, vectorset_id: str):
        """Delete a vectorset from all KB shards"""

        async def _delete_vectorset(node: AbstractIndexNode, shard_id: str):
            result = await node.remove_vectorset(shard_id, vectorset_id)
            if result.status != result.Status.OK:
                raise NodeError(
                    f"Unable to delete vectorset {vectorset_id} in kb {kbid} shard {shard_id}"
                )

        await self.apply_for_all_shards(
            kbid, _delete_vectorset, timeout=10, use_read_replica_nodes=False
        )


class StandaloneKBShardManager(KBShardManager):
    max_ops_before_checks = 200

    def __init__(self):
        super().__init__()
        self._lock = asyncio.Lock()
        self._change_count: dict[tuple[str, str], int] = {}

    async def _resource_change_event(self, kbid: str, node_id: str, shard_id: str) -> None:
        if (node_id, shard_id) not in self._change_count:
            self._change_count[(node_id, shard_id)] = 0
        self._change_count[(node_id, shard_id)] += 1
        if self._change_count[(node_id, shard_id)] < self.max_ops_before_checks:
            return

        self._change_count[(node_id, shard_id)] = 0
        async with self._lock:
            index_node: Optional[ProxyStandaloneIndexNode] = get_index_node(node_id)  # type: ignore
            if index_node is None:
                return
            shard_info: noderesources_pb2.Shard = await index_node.reader.GetShard(
                nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=shard_id))
            )
            await self.maybe_create_new_shard(
                kbid,
                shard_info.paragraphs,
            )
            await index_node.writer.GC(noderesources_pb2.ShardId(id=shard_id))

    @backoff.on_exception(backoff.expo, NodesUnsync, jitter=backoff.random_jitter, max_tries=5)
    async def delete_resource(
        self,
        shard: writer_pb2.ShardObject,
        uuid: str,
        txid: int,
        partition: str,
        kb: str,
    ) -> None:
        req = noderesources_pb2.ResourceID()
        req.uuid = uuid

        for shardreplica in shard.replicas:
            req.shard_id = shardreplica.shard.id
            index_node = get_index_node(shardreplica.node)
            if index_node is None:  # pragma: no cover
                raise NodesUnsync(f"Node {shardreplica.node} is not found or not available")
            await index_node.writer.RemoveResource(req)  # type: ignore
            asyncio.create_task(
                self._resource_change_event(kb, shardreplica.node, shardreplica.shard.id)
            )

    @backoff.on_exception(backoff.expo, NodesUnsync, jitter=backoff.random_jitter, max_tries=5)
    async def add_resource(
        self,
        shard: writer_pb2.ShardObject,
        resource: noderesources_pb2.Resource,
        txid: int,
        partition: str,
        kb: str,
        reindex_id: Optional[str] = None,
        source: IndexMessageSource.ValueType = IndexMessageSource.PROCESSOR,
    ) -> None:
        """
        Calls the node writer's SetResource method directly to store the resource in the node.
        There is no queuing for standalone nodes at the moment -- indexing is done synchronously.
        """
        index_node = None
        for shardreplica in shard.replicas:
            resource.shard_id = resource.resource.shard_id = shardreplica.shard.id
            index_node = get_index_node(shardreplica.node)
            if index_node is None:  # pragma: no cover
                raise NodesUnsync(f"Node {shardreplica.node} is not found or not available")
            await index_node.writer.SetResource(resource)  # type: ignore
            asyncio.create_task(
                self._resource_change_event(kb, shardreplica.node, shardreplica.shard.id)
            )


def get_all_shard_nodes(
    shard: writer_pb2.ShardObject,
    *,
    use_read_replicas: bool,
) -> list[tuple[AbstractIndexNode, str]]:
    """Return a list of all nodes containing `shard` with the shard replica id.
    If `use_read_replicas`, read replica nodes will be returned too.

    """
    nodes = []
    for shard_replica_pb in shard.replicas:
        node_id = shard_replica_pb.node
        shard_replica_id = shard_replica_pb.shard.id

        node = get_index_node(node_id)
        if node is not None:
            nodes.append((node, shard_replica_id))

        if use_read_replicas:
            for read_replica_node_id in get_read_replica_node_ids(node_id):
                read_replica_node = get_index_node(read_replica_node_id)
                if read_replica_node is not None:
                    nodes.append((read_replica_node, shard_replica_id))

    return nodes


def choose_node(
    shard: writer_pb2.ShardObject,
    *,
    target_shard_replicas: Optional[list[str]] = None,
    use_read_replica_nodes: bool = False,
) -> tuple[AbstractIndexNode, str]:
    """Choose an arbitrary node storing `shard` following these rules:
    - nodes containing a shard replica from `target_replicas` are the preferred
    - when enabled, read replica nodes are preferred over primaries
    - if there's more than one option with the same score, a random choice will
      be made between them.

    According to these rules and considering we use read replica nodes, a read
    replica node containing a shard replica from `target_shard_replicas` is the
    most preferent, while a primary node with a shard not in
    `target_shard_replicas` is the least preferent.

    """
    target_shard_replicas = target_shard_replicas or []

    shard_nodes = get_all_shard_nodes(shard, use_read_replicas=use_read_replica_nodes)

    if len(shard_nodes) == 0:
        raise NoHealthyNodeAvailable("Could not find a node to query")

    # Ranking values
    IN_TARGET_SHARD_REPLICAS = 0b10
    IS_READ_REPLICA_NODE = 0b01

    ranked_nodes: dict[int, list[tuple[AbstractIndexNode, str]]] = {}
    for node, shard_replica_id in shard_nodes:
        score = 0
        if shard_replica_id in target_shard_replicas:
            score |= IN_TARGET_SHARD_REPLICAS
        if node.is_read_replica():
            score |= IS_READ_REPLICA_NODE

        ranked_nodes.setdefault(score, []).append((node, shard_replica_id))

    top = ranked_nodes[max(ranked_nodes)]
    # As shard replica ids are random numbers, we sort by shard replica id and choose its
    # node to make sure we choose in deterministically but we don't favour any node in particular
    top.sort(key=lambda x: x[1])
    selected_node, shard_replica_id = top[0]
    return selected_node, shard_replica_id


def check_enough_nodes():
    """
    It raises an exception if it can't find enough nodes for the configured replicas.
    """
    drain_nodes = settings.drain_nodes
    target_replicas = settings.node_replicas
    available_nodes = get_index_nodes()
    available_nodes = [node for node in available_nodes if node.id not in drain_nodes]
    if len(available_nodes) < target_replicas:
        raise NodeClusterSmall(
            f"Not enough nodes. Total: {len(available_nodes)}, Required: {target_replicas}"
        )
    if settings.max_node_replicas >= 0:
        available_nodes = list(
            filter(lambda n: n.shard_count < settings.max_node_replicas, available_nodes)
        )
        if len(available_nodes) < target_replicas:
            raise NodeClusterSmall(
                f"Could not find enough nodes with available shards. Available: {len(available_nodes)}, Required: {target_replicas}"  # noqa
            )


def sorted_primary_nodes(
    avoid_nodes: Optional[list[str]] = None,
    ignore_nodes: Optional[list[str]] = None,
) -> list[str]:
    """
    Returns the list of all primary node ids sorted by decreasing available
    disk space (from more to less available disk reported).

    Nodes in `avoid_nodes` are placed at the tail of the list.
    Nodes in `ignore_nodes` are ignored and never returned.
    """
    primary_nodes = get_index_nodes(include_secondary=False)

    # Sort by available disk
    sorted_nodes = sorted(primary_nodes, key=lambda n: n.available_disk, reverse=True)
    available_node_ids = [node.id for node in sorted_nodes]

    avoid_nodes = avoid_nodes or []
    ignore_nodes = ignore_nodes or []

    # Get the non-avoided nodes first
    preferred_nodes = [nid for nid in available_node_ids if nid not in avoid_nodes]

    # Add avoid_nodes to the end of the last nodes
    result_nodes = preferred_nodes + [nid for nid in available_node_ids if nid not in preferred_nodes]

    # Remove ignore_nodes from the list
    result_nodes = [nid for nid in result_nodes if nid not in ignore_nodes]
    return result_nodes
