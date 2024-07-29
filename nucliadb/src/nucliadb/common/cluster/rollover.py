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
import argparse
import asyncio
import enum
import logging
from datetime import datetime
from typing import Optional

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.context import ApplicationContext
from nucliadb_protos import nodewriter_pb2, writer_pb2
from nucliadb_telemetry import errors

from .manager import get_index_node
from .settings import settings
from .utils import delete_resource_from_shard, index_resource_to_shard, wait_for_node

logger = logging.getLogger(__name__)


class RolloverStatus(enum.Enum):
    RESOURCES_SCHEDULED = "resources_scheduled"
    RESOURCES_INDEXED = "resources_indexed"
    RESOURCES_VALIDATED = "resources_validated"


def _get_rollover_status(rollover_shards: writer_pb2.Shards, status: RolloverStatus) -> bool:
    return rollover_shards.extra.get(status.value) == "true"


def _set_rollover_status(rollover_shards: writer_pb2.Shards, status: RolloverStatus):
    rollover_shards.extra[status.value] = "true"


def _clear_rollover_status(rollover_shards: writer_pb2.Shards):
    for status in RolloverStatus:
        rollover_shards.extra.pop(status.value, None)


class UnexpectedRolloverError(Exception):
    pass


async def create_rollover_shards(
    app_context: ApplicationContext, kbid: str, drain_nodes: Optional[list[str]] = None
) -> writer_pb2.Shards:
    """
    Creates shards to be used for a rollover operation.
    If drain_nodes is provided, no replicas will be created on those nodes.
    """
    logger.info("Creating rollover shards", extra={"kbid": kbid})
    sm = app_context.shard_manager

    async with datamanagers.with_ro_transaction() as txn:
        existing_rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
        if existing_rollover_shards is not None:
            logger.info("Rollover shards already exist, skipping", extra={"kbid": kbid})
            return existing_rollover_shards

        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if kb_shards is None:
            raise UnexpectedRolloverError(f"No shards found for KB {kbid}")

    # create new shards
    created_shards = []
    try:
        nodes = cluster_manager.sorted_primary_nodes(ignore_nodes=drain_nodes)
        for shard in kb_shards.shards:
            shard.ClearField("replicas")
            # Attempt to create configured number of replicas
            replicas_created = 0
            while replicas_created < settings.node_replicas:
                if len(nodes) == 0:
                    # could have multiple shards on single node
                    nodes = cluster_manager.sorted_primary_nodes(ignore_nodes=drain_nodes)
                node_id = nodes.pop(0)

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
                    logger.exception(f"Error creating new shard at {node}")
                    continue

                replica = writer_pb2.ShardReplica(node=str(node_id))
                replica.shard.CopyFrom(shard_created)
                shard.replicas.append(replica)
                created_shards.append(shard)
                replicas_created += 1
    except Exception as e:
        errors.capture_exception(e)
        logger.exception("Unexpected error creating new shard")
        for created_shard in created_shards:
            await sm.rollback_shard(created_shard)
        raise e

    async with datamanagers.with_transaction() as txn:
        await datamanagers.rollover.update_kb_rollover_shards(txn, kbid=kbid, kb_shards=kb_shards)
        await txn.commit()
        return kb_shards


def _get_shard(shards: writer_pb2.Shards, shard_id: str) -> Optional[writer_pb2.ShardObject]:
    for shard in shards.shards:
        if shard_id == shard.shard:
            return shard
    return None


async def schedule_resource_indexing(app_context: ApplicationContext, kbid: str) -> None:
    """
    Schedule indexing all data in a kb in rollover shards
    """
    logger.info("Indexing rollover shards", extra={"kbid": kbid})

    async with datamanagers.with_transaction() as txn:
        rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
        if rollover_shards is None:
            raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rollover_shards, RolloverStatus.RESOURCES_SCHEDULED):
        logger.info(
            "Resources already scheduled for indexing, skipping",
            extra={"kbid": kbid},
        )
        return

    batch = []
    async for resource_id in datamanagers.resources.iterate_resource_ids(kbid=kbid):
        batch.append(resource_id)

        if len(batch) > 100:
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.add_batch_to_index(txn, kbid=kbid, batch=batch)
                await txn.commit()
            batch = []
    if len(batch) > 0:
        async with datamanagers.with_transaction() as txn:
            await datamanagers.rollover.add_batch_to_index(txn, kbid=kbid, batch=batch)
            await txn.commit()

    async with datamanagers.with_transaction() as txn:
        _set_rollover_status(rollover_shards, RolloverStatus.RESOURCES_SCHEDULED)
        await datamanagers.rollover.update_kb_rollover_shards(txn, kbid=kbid, kb_shards=rollover_shards)
        await txn.commit()


def _to_ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000 * 1000)


async def index_rollover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Indexes all data in a kb in rollover shards
    """

    async with datamanagers.with_transaction() as txn:
        rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
    if rollover_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rollover_shards, RolloverStatus.RESOURCES_INDEXED):
        logger.info("Resources already indexed, skipping", extra={"kbid": kbid})
        return

    logger.info("Indexing rollover shards", extra={"kbid": kbid})

    wait_index_batch: list[writer_pb2.ShardObject] = []
    # now index on all new shards only
    while True:
        async with datamanagers.with_transaction() as txn:
            resource_id = await datamanagers.rollover.get_to_index(txn, kbid=kbid)
        if resource_id is None:
            break

        async with datamanagers.with_transaction() as txn:
            shard_id = await datamanagers.resources.get_resource_shard_id(
                txn, kbid=kbid, rid=resource_id
            )
        if shard_id is None:
            logger.warning(
                "Shard id not found for resource. Skipping indexing as it may have been deleted",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.remove_to_index(txn, kbid=kbid, resource=resource_id)
                await txn.commit()
            continue

        shard = _get_shard(rollover_shards, shard_id)
        if shard is None:  # pragma: no cover
            logger.error(
                "Shard not found for resource",
                extra={"kbid": kbid, "resource_id": resource_id, "shard_id": shard_id},
            )
            raise UnexpectedRolloverError(
                f"Shard {shard_id} not found. Was a new one created during migration?"
            )

        resource = await index_resource_to_shard(app_context, kbid, resource_id, shard)
        if resource is None:
            # resource no longer existing, remove indexing and carry on
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.remove_to_index(txn, kbid=kbid, resource=resource_id)
                await txn.commit()
            continue

        async with datamanagers.with_transaction() as txn:
            await datamanagers.rollover.add_indexed(
                txn,
                kbid=kbid,
                resource_id=resource_id,
                shard_id=shard_id,
                modification_time=_to_ts(resource.basic.modified.ToDatetime()),  # type: ignore
            )
            await txn.commit()
        wait_index_batch.append(shard)

        if len(wait_index_batch) > 10:
            node_ids = set()
            for shard_batch in wait_index_batch:
                for replica in shard_batch.replicas:
                    node_ids.add(replica.node)
            for node_id in node_ids:
                await wait_for_node(app_context, node_id)
            wait_index_batch = []

    _set_rollover_status(rollover_shards, RolloverStatus.RESOURCES_INDEXED)
    async with datamanagers.with_transaction() as txn:
        await datamanagers.rollover.update_kb_rollover_shards(txn, kbid=kbid, kb_shards=rollover_shards)
        await txn.commit()


async def cutover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Swaps our the current active shards for a knowledgebox.
    """
    logger.info("Cutting over shards", extra={"kbid": kbid})
    async with datamanagers.with_transaction() as txn:
        sm = app_context.shard_manager

        previously_active_shards = await datamanagers.cluster.get_kb_shards(
            txn, kbid=kbid, for_update=True
        )
        rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
        if previously_active_shards is None or rollover_shards is None:
            raise UnexpectedRolloverError("Shards for kb not found")

        _clear_rollover_status(rollover_shards)
        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=rollover_shards)
        await datamanagers.rollover.delete_kb_rollover_shards(txn, kbid=kbid)

        for shard in previously_active_shards.shards:
            await sm.rollback_shard(shard)

        await txn.commit()


async def validate_indexed_data(app_context: ApplicationContext, kbid: str) -> list[str]:
    """
    Goes through all the resources in a knowledgebox and validates it
    against the data that was indexed during the rollover.

    If any resource is missing, it will be indexed again.

    If a resource was removed during the rollover, it will be removed as well.
    """

    async with datamanagers.with_ro_transaction() as txn:
        rolled_over_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if rolled_over_shards is None:
            raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rolled_over_shards, RolloverStatus.RESOURCES_VALIDATED):
        logger.info("Resources already validated, skipping", extra={"kbid": kbid})
        return []

    logger.info("Validating indexed data", extra={"kbid": kbid})

    repaired_resources = []
    async for resource_id in datamanagers.resources.iterate_resource_ids(kbid=kbid):
        async with datamanagers.with_ro_transaction() as txn:
            indexed_data = await datamanagers.rollover.get_indexed_data(
                txn, kbid=kbid, resource_id=resource_id
            )

        if indexed_data is not None:
            shard_id, last_indexed = indexed_data
            if last_indexed == -1:
                continue
        else:
            async with datamanagers.with_transaction() as txn:
                shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=kbid, rid=resource_id
                )  # type: ignore
            if shard_id is None:
                logger.error(
                    "Shard id not found for resource",
                    extra={"kbid": kbid, "resource_id": resource_id},
                )
                raise UnexpectedRolloverError("Shard id not found for resource")
            last_indexed = 0

        shard = _get_shard(rolled_over_shards, shard_id)
        if shard is None:
            logger.error(
                "Shard not found for resource",
                extra={
                    "kbid": kbid,
                    "resource_id": resource_id,
                    "shard_id": shard_id,
                },
            )
            raise UnexpectedRolloverError(f"Shard {shard_id} not found. This should not happen")

        async with datamanagers.with_transaction() as txn:
            res = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=resource_id)
        if res is None:
            logger.error(
                "Resource not found while validating, skipping",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            continue

        if _to_ts(res.basic.modified.ToDatetime()) <= last_indexed:  # type: ignore
            # resource was not affected by rollover, carry on
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.add_indexed(
                    txn,
                    kbid=kbid,
                    resource_id=resource_id,
                    shard_id=shard_id,
                    modification_time=-1,
                )
                await txn.commit()
            continue

        # resource was modified or added during rollover, reindex
        resource = await index_resource_to_shard(app_context, kbid, resource_id, shard)
        if resource is not None:
            repaired_resources.append(resource_id)
        async with datamanagers.with_transaction() as txn:
            await datamanagers.rollover.add_indexed(
                txn,
                kbid=kbid,
                resource_id=resource_id,
                shard_id=shard_id,
                modification_time=-1,
            )
            await txn.commit()

    # any left overs should be deleted
    async for resource_id, (
        shard_id,
        last_indexed,
    ) in datamanagers.rollover.iterate_indexed_data(kbid=kbid):
        if last_indexed == -1:
            continue

        shard = _get_shard(rolled_over_shards, shard_id)
        if shard is None:
            raise UnexpectedRolloverError("Shard not found. This should not happen")
        await delete_resource_from_shard(app_context, kbid, resource_id, shard)

    _set_rollover_status(rolled_over_shards, RolloverStatus.RESOURCES_VALIDATED)
    async with datamanagers.with_transaction() as txn:
        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=rolled_over_shards)

    return repaired_resources


async def clean_indexed_data(app_context: ApplicationContext, kbid: str) -> None:
    batch = []
    async for key in datamanagers.rollover.iter_indexed_keys(kbid=kbid):
        batch.append(key)
        if len(batch) >= 100:
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.remove_indexed(txn, kbid=kbid, batch=batch)
                await txn.commit()
            batch = []
    if len(batch) >= 0:
        async with datamanagers.with_transaction() as txn:
            await datamanagers.rollover.remove_indexed(txn, kbid=kbid, batch=batch)
            await txn.commit()


async def clean_rollover_status(app_context: ApplicationContext, kbid: str) -> None:
    async with datamanagers.with_transaction() as txn:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
        if kb_shards is None:
            logger.warning(
                "No shards found for KB, skipping clean rollover status",
                extra={"kbid": kbid},
            )
            return

        _clear_rollover_status(kb_shards)
        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)


async def rollover_kb_shards(
    app_context: ApplicationContext, kbid: str, drain_nodes: Optional[list[str]] = None
) -> None:
    """
    Rollover a shard is the process of creating new shard replicas for every
    shard and indexing all existing resources into the replicas.

    Once all the data is in the new shards, cut over the registered replicas
    to the new shards and delete the old shards.

    If drain_nodes is provided, no replicas will be created on those nodes. This is useful
    for when we want to remove a set of nodes from the cluster.

    This is a very expensive operation and should be done with care.

    Process:
    - Create new shards
    - Schedule all resources to be indexed
    - Index all resources into new shards
    - Cut over replicas to new shards
    - Validate that all resources are in the new shards
    - Clean up indexed data
    """
    node_ready_checks = 0
    while len(cluster_manager.INDEX_NODES) == 0:
        if node_ready_checks > 10:
            raise Exception("No index nodes available")
        logger.info("Waiting for index nodes to be available")
        await asyncio.sleep(1)
        node_ready_checks += 1

    logger.info("Rolling over shards", extra={"kbid": kbid})

    async with locking.distributed_lock(locking.KB_SHARDS_LOCK.format(kbid=kbid)):
        await create_rollover_shards(app_context, kbid, drain_nodes=drain_nodes)
        await schedule_resource_indexing(app_context, kbid)
        await index_rollover_shards(app_context, kbid)
        await cutover_shards(app_context, kbid)
        # we need to cut over BEFORE we validate the data
        await validate_indexed_data(app_context, kbid)
        await clean_indexed_data(app_context, kbid)
        await clean_rollover_status(app_context, kbid)

    logger.info("Finished rolling over shards", extra={"kbid": kbid})


async def _rollover_kbid_command(kbid: str) -> None:  # pragma: no cover
    app_context = ApplicationContext()
    await app_context.initialize()
    try:
        await rollover_kb_shards(app_context, kbid)
    finally:
        await app_context.finalize()


argparser = argparse.ArgumentParser()
argparser.add_argument("--kbid", help="Knowledge base ID to rollover", required=True)


def rollover_kbid_command() -> None:  # pragma: no cover
    args = argparser.parse_args()
    asyncio.run(_rollover_kbid_command(args.kbid))
