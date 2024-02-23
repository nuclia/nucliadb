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

import argparse
import asyncio
from dataclasses import dataclass
from typing import Optional

import pkg_resources
from grpc.aio import AioRpcError  # type: ignore

from nucliadb.common.cluster import manager
from nucliadb.common.cluster.base import AbstractIndexNode
from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import KBShardManager
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.datamanagers.rollover import RolloverDataManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.ingest import logger
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging


@dataclass
class ShardLocation:
    kbid: str
    node_id: str


UNKNOWN_KB = "unknown"


async def detect_orphan_shards(driver: Driver) -> dict[str, ShardLocation]:
    """Detect orphan shards in the system. An orphan shard is one indexed but
    not referenced for any stored KB.

    """
    # To avoid detecting a new shard as orphan, query the index first and maindb
    # afterwards
    indexed_shards: dict[str, ShardLocation] = {}
    available_nodes = manager.get_index_nodes()
    for node in available_nodes:
        node_shards = await _get_indexed_shards(node)
        indexed_shards.update(node_shards)

    stored_shards = await _get_stored_shards(driver)

    # Log an error in case we found a shard stored but not indexed, this should
    # never happen as shards are created in the index node and then stored in
    # maindb
    not_indexed_shards = stored_shards.keys() - indexed_shards.keys()
    available_nodes_ids = [node.id for node in available_nodes]
    for shard_id in not_indexed_shards:
        location = stored_shards[shard_id]

        # skip shards from unavailable nodes
        if location.node_id not in available_nodes_ids:
            continue

        logger.error(
            "Found a shard on maindb not indexed in the index nodes",
            extra={
                "shard_id": shard_id,
                "kbid": location.kbid,
                "node_id": location.node_id,
            },
        )

    orphan_shard_ids = indexed_shards.keys() - stored_shards.keys()
    orphan_shards: dict[str, ShardLocation] = {}
    unavailable_nodes: set[str] = set()
    rollover_dm = RolloverDataManager(driver)
    for shard_id in orphan_shard_ids:
        node_id = indexed_shards[shard_id].node_id
        node = manager.get_index_node(node_id)  # type: ignore
        if node is None:
            unavailable_nodes.add(node_id)
            kbid = UNKNOWN_KB
        else:
            kbid = await _get_kbid(node, shard_id) or UNKNOWN_KB

        # Shards with knwon KB ids can be checked and ignore those comming from
        # an ongoing migration/rollover
        if kbid != UNKNOWN_KB:
            skip = await rollover_dm.is_rollover_shard(kbid, shard_id)
            if skip:
                continue

        orphan_shards[shard_id] = ShardLocation(kbid=kbid, node_id=node_id)

    if len(unavailable_nodes) > 0:
        logger.info(
            "Some nodes were unavailable while checking shard details and were skipped",
            extra={"nodes": list(unavailable_nodes)},
        )

    return orphan_shards


async def _get_indexed_shards(node: AbstractIndexNode) -> dict[str, ShardLocation]:
    indexed_shards: dict[str, ShardLocation] = {}
    node_shards = await node.list_shards()
    for shard_id in node_shards:
        indexed_shards[shard_id] = ShardLocation(kbid=UNKNOWN_KB, node_id=node.id)
    return indexed_shards


async def _get_stored_shards(driver: Driver) -> dict[str, ShardLocation]:
    stored_shards: dict[str, ShardLocation] = {}
    shards_manager = KBShardManager()

    async with driver.transaction(read_only=True) as txn:
        async for kbid, _ in KnowledgeBox.get_kbs(txn, slug=""):
            try:
                kb_shards = await shards_manager.get_shards_by_kbid(kbid)
            except ShardsNotFound:
                logger.warning(
                    "KB not found while looking for orphan shards", extra={"kbid": kbid}
                )
                continue
            else:
                for shard_object_pb in kb_shards:
                    for shard_replica_pb in shard_object_pb.replicas:
                        shard_replica_id = shard_replica_pb.shard.id
                        node_id = shard_replica_pb.node
                        stored_shards[shard_replica_id] = ShardLocation(
                            kbid=kbid, node_id=node_id
                        )
    return stored_shards


async def _get_kbid(node: AbstractIndexNode, shard_id: str) -> Optional[str]:
    kbid = None
    try:
        shard_pb = await node.get_shard(shard_id)
    except AioRpcError as grpc_error:
        logger.error(
            "Can't get shard while looking for orphans in index nodes, is it broken?",
            exc_info=grpc_error,
            extra={
                "node_id": node.id,
                "shard_id": shard_id,
            },
        )
    else:
        kbid = shard_pb.metadata.kbid
    return kbid


async def report_orphan_shards(driver: Driver):
    orphan_shards = await detect_orphan_shards(driver)
    logger.info(f"Found {len(orphan_shards)} orphan shards")
    async with driver.transaction(read_only=True) as txn:
        for shard_id, location in orphan_shards.items():
            if location.kbid == UNKNOWN_KB:
                msg = "Found orphan shard but could not get KB info"
            else:
                kb_exists = await KnowledgeBox.exist_kb(txn, location.kbid)
                if kb_exists:
                    msg = "Found orphan shard for existing KB"
                else:
                    msg = "Found orphan shard for already removed KB"

            logger.debug(
                msg,
                extra={
                    "shard_id": shard_id,
                    "kbid": location.kbid,
                    "node_id": location.node_id,
                },
            )


async def purge_orphan_shards(driver: Driver):
    orphan_shards = await detect_orphan_shards(driver)
    logger.info(f"Found {len(orphan_shards)} orphan shards. Purge starts...")

    unavailable_nodes: set[str] = set()
    for shard_id, location in orphan_shards.items():
        node = manager.get_index_node(location.node_id)
        if node is None:
            unavailable_nodes.add(location.node_id)
            continue

        logger.info(
            "Deleting orphan shard from index node",
            extra={
                "shard_id": shard_id,
                "kbid": location.kbid,
                "node_id": location.node_id,
            },
        )
        await node.delete_shard(shard_id)

    for node_id in unavailable_nodes:
        logger.warning(
            "Index node has been unavailable while purging. Orphan shards may still exist",
            extra={"node_id": node_id},
        )


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--purge",
        action="store_true",
        default=False,
        required=False,
        help="Purge detected orphan shards",
    )
    args = parser.parse_args()
    return args


async def main():
    """This script will detect orphan shards, i.e., indexed shards with no
    reference in our source of truth.

    Purging a knowledgebox can lead to orphan shards if index nodes are not
    available or fail. It's possible that other procedures in our database left
    orphan shards too.

    ATENTION! In the future, some new process which adds new shards could be
    implemented. If orphan shard detection is not updated, this will lead to
    incorrect detection. To avoid data loss/corruption, it is highly recommended
    to don't remove orphan shards that hasn't exist for a long time.

    """
    args = parse_arguments()

    await setup_cluster()
    driver = await setup_driver()

    try:
        if args.purge:
            await purge_orphan_shards(driver)
        else:
            await report_orphan_shards(driver)

    finally:
        await teardown_driver()
        await teardown_cluster()


def run() -> int:  # pragma: no cover
    setup_logging()

    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)

    return asyncio.run(main())
