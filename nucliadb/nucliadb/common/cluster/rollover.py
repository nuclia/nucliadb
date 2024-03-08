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

import backoff

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.cluster import ClusterDataManager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.datamanagers.rollover import RolloverDataManager
from nucliadb_protos import noderesources_pb2, writer_pb2
from nucliadb_telemetry import errors
from nucliadb_utils import const

from .manager import get_index_node
from .settings import settings

logger = logging.getLogger(__name__)


class RolloverStatus(enum.Enum):
    RESOURCES_SCHEDULED = "resources_scheduled"
    RESOURCES_INDEXED = "resources_indexed"
    RESOURCES_VALIDATED = "resources_validated"


def _get_rollover_status(
    rollover_shards: writer_pb2.Shards, status: RolloverStatus
) -> bool:
    return rollover_shards.extra.get(status.value) == "true"


def _set_rollover_status(rollover_shards: writer_pb2.Shards, status: RolloverStatus):
    rollover_shards.extra[status.value] = "true"


def _clear_rollover_status(rollover_shards: writer_pb2.Shards):
    for status in RolloverStatus:
        rollover_shards.extra.pop(status.value, None)


class UnexpectedRolloverError(Exception):
    pass


async def create_rollover_shards(
    app_context: ApplicationContext, kbid: str
) -> writer_pb2.Shards:
    """
    Creates shards to to used for a rollover operation
    """
    logger.warning("Creating rollover shards", extra={"kbid": kbid})
    sm = app_context.shard_manager
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)

    existing_rollover_shards = await rollover_datamanager.get_kb_rollover_shards(kbid)
    if existing_rollover_shards is not None:
        logger.warning("Rollover shards already exist, skipping")
        return existing_rollover_shards

    kb_shards = await cluster_datamanager.get_kb_shards(kbid)
    if kb_shards is None:
        raise UnexpectedRolloverError(f"No shards found for KB {kbid}")

    # create new shards
    created_shards = []
    try:
        nodes = cluster_manager.sorted_primary_nodes()
        for shard in kb_shards.shards:
            shard.ClearField("replicas")
            # Attempt to create configured number of replicas
            replicas_created = 0
            while replicas_created < settings.node_replicas:
                if len(nodes) == 0:
                    # could have multiple shards on single node
                    nodes = cluster_manager.sorted_primary_nodes()
                node_id = nodes.pop(0)

                node = get_index_node(node_id)
                if node is None:
                    logger.error(f"Node {node_id} is not found or not available")
                    continue
                try:
                    shard_created = await node.new_shard(
                        kbid,
                        similarity=kb_shards.similarity,
                        release_channel=kb_shards.release_channel,
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

    await rollover_datamanager.update_kb_rollover_shards(kbid, kb_shards)
    return kb_shards


def _get_shard(
    shards: writer_pb2.Shards, shard_id: str
) -> Optional[writer_pb2.ShardObject]:
    for shard in shards.shards:
        if shard_id == shard.shard:
            return shard
    return None


async def wait_for_node(app_context: ApplicationContext, node_id: str) -> None:
    while True:
        # get raw js client
        js = getattr(app_context.nats_manager.js, "js", app_context.nats_manager.js)
        consumer_info = await js.consumer_info(
            const.Streams.INDEX.name, const.Streams.INDEX.group.format(node=node_id)
        )
        if consumer_info.num_pending < 5:
            logger.info(f"Node is ready to consume messages.", extra={"node": node_id})
            return

        logger.info(
            f"Waiting for node to consume messages. {consumer_info.num_pending} messages left.",
            extra={"node": node_id},
        )
        # usually we consume around 3-4 messages/s with some eventual peaks of
        # 10-30. If there are too many pending messages, we can wait more.
        # We suppose 5 messages/s and don't wait more than 60s
        sleep = min(max(2, consumer_info.num_pending / 5), 60)
        await asyncio.sleep(sleep)


@backoff.on_exception(
    backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=8
)
async def index_resource(
    app_context: ApplicationContext,
    kbid: str,
    resource_id: str,
    shard: writer_pb2.ShardObject,
) -> Optional[noderesources_pb2.Resource]:
    logger.warning(
        "Indexing resource", extra={"kbid": kbid, "resource_id": resource_id}
    )

    sm = app_context.shard_manager
    partitioning = app_context.partitioning
    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )

    resource_index_message = await resources_datamanager.get_resource_index_message(
        kbid, resource_id
    )
    if resource_index_message is None:
        logger.warning(
            "Resource index message not found while indexing, skipping",
            extra={"kbid": kbid, "resource_id": resource_id},
        )
        return None
    partition = partitioning.generate_partition(kbid, resource_id)
    await sm.add_resource(
        shard, resource_index_message, txid=-1, partition=str(partition), kb=kbid
    )
    return resource_index_message


async def schedule_resource_indexing(
    app_context: ApplicationContext, kbid: str
) -> None:
    """
    Schedule indexing all data in a kb in rollover shards
    """
    logger.warning("Indexing rollover shards", extra={"kbid": kbid})

    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)

    rollover_shards = await rollover_datamanager.get_kb_rollover_shards(kbid)
    if rollover_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rollover_shards, RolloverStatus.RESOURCES_SCHEDULED):
        logger.warning(
            "Resources already scheduled for indexing, skipping", extra={"kbid": kbid}
        )
        return

    batch = []
    async for resource_id in resources_datamanager.iterate_resource_ids(kbid):
        batch.append(resource_id)

        if len(batch) > 100:
            await rollover_datamanager.add_batch_to_index(kbid, batch)
            batch = []
    if len(batch) > 0:
        await rollover_datamanager.add_batch_to_index(kbid, batch)

    _set_rollover_status(rollover_shards, RolloverStatus.RESOURCES_SCHEDULED)
    await rollover_datamanager.update_kb_rollover_shards(kbid, rollover_shards)


def _to_ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000 * 1000)


async def index_rollover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Indexes all data in a kb in rollover shards
    """
    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)

    rollover_shards = await rollover_datamanager.get_kb_rollover_shards(kbid)
    if rollover_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rollover_shards, RolloverStatus.RESOURCES_INDEXED):
        logger.warning("Resources already indexed, skipping", extra={"kbid": kbid})
        return

    logger.warning("Indexing rollover shards", extra={"kbid": kbid})

    wait_index_batch: list[writer_pb2.ShardObject] = []
    # now index on all new shards only
    while True:
        resource_id = await rollover_datamanager.get_to_index(kbid)
        if resource_id is None:
            break

        shard_id = await resources_datamanager.get_resource_shard_id(kbid, resource_id)
        if shard_id is None:
            logger.error(
                "Shard id not found for resource",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            raise UnexpectedRolloverError("Shard id not found for resource")

        shard = _get_shard(rollover_shards, shard_id)
        if shard is None:  # pragma: no cover
            logger.error(
                "Shard not found for resource",
                extra={"kbid": kbid, "resource_id": resource_id, "shard_id": shard_id},
            )
            raise UnexpectedRolloverError(
                f"Shard {shard_id} not found. Was a new one created during migration?"
            )

        resource_index_message = await index_resource(
            app_context, kbid, resource_id, shard
        )
        if resource_index_message is None:
            # resource no longer existing, remove indexing and carry on
            await rollover_datamanager.remove_to_index(kbid, resource_id)
            continue

        await rollover_datamanager.add_indexed(
            kbid,
            resource_id,
            shard_id,
            _to_ts(resource_index_message.metadata.modified.ToDatetime()),
        )
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
    await rollover_datamanager.update_kb_rollover_shards(kbid, rollover_shards)


async def cutover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Swaps our the current active shards for a knowledgebox.
    """
    logger.warning("Cutting over shards", extra={"kbid": kbid})
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)
    sm = app_context.shard_manager

    previously_active_shards = await cluster_datamanager.get_kb_shards(kbid)
    rollover_shards = await rollover_datamanager.get_kb_rollover_shards(kbid)
    if previously_active_shards is None or rollover_shards is None:
        raise UnexpectedRolloverError("Shards for kb not found")

    _clear_rollover_status(rollover_shards)
    await cluster_datamanager.update_kb_shards(kbid, rollover_shards)
    await rollover_datamanager.delete_kb_rollover_shards(kbid)

    for shard in previously_active_shards.shards:
        await sm.rollback_shard(shard)


async def validate_indexed_data(
    app_context: ApplicationContext, kbid: str
) -> list[str]:
    """
    Goes through all the resources in a knowledgebox and validates it
    against the data that was indexed during the rollover.

    If any resource is missing, it will be indexed again.

    If a resource was removed during the rollover, it will be removed as well.
    """
    sm = app_context.shard_manager
    partitioning = app_context.partitioning

    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)

    rolled_over_shards = await cluster_datamanager.get_kb_shards(kbid)
    if rolled_over_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if _get_rollover_status(rolled_over_shards, RolloverStatus.RESOURCES_VALIDATED):
        logger.warning("Resources already validated, skipping", extra={"kbid": kbid})
        return []

    logger.warning("Validating indexed data", extra={"kbid": kbid})

    repaired_resources = []
    async for resource_id in resources_datamanager.iterate_resource_ids(kbid):
        indexed_data = await rollover_datamanager.get_indexed_data(kbid, resource_id)

        if indexed_data is not None:
            shard_id, last_indexed = indexed_data
            if last_indexed == -1:
                continue
        else:
            shard_id = await resources_datamanager.get_resource_shard_id(
                kbid, resource_id
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
                extra={"kbid": kbid, "resource_id": resource_id, "shard_id": shard_id},
            )
            raise UnexpectedRolloverError(
                f"Shard {shard_id} not found. This should not happen"
            )

        res = await resources_datamanager.get_resource(kbid, resource_id)
        if res is None:
            logger.error(
                "Resource not found while validating, skipping",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            continue

        if _to_ts(res.basic.modified.ToDatetime()) <= last_indexed:  # type: ignore
            # resource was not affected by rollover, carry on
            await rollover_datamanager.add_indexed(kbid, resource_id, shard_id, -1)
            continue

        # resource was modified or added during rollover, reindex
        resource_index_message = await index_resource(
            app_context, kbid, resource_id, shard
        )
        if resource_index_message is not None:
            repaired_resources.append(resource_id)
        await rollover_datamanager.add_indexed(kbid, resource_id, shard_id, -1)

    # any left overs should be deleted
    async for resource_id, (
        shard_id,
        last_indexed,
    ) in rollover_datamanager.iterate_indexed_data(kbid):
        if last_indexed == -1:
            continue

        shard = _get_shard(rolled_over_shards, shard_id)
        if shard is None:
            raise UnexpectedRolloverError("Shard not found. This should not happen")
        logger.warning(
            "Deleting resource from index",
            extra={"kbid": kbid, "resource_id": resource_id},
        )
        partition = partitioning.generate_partition(kbid, resource_id)
        await sm.delete_resource(shard, resource_id, -1, str(partition), kbid)

    _set_rollover_status(rolled_over_shards, RolloverStatus.RESOURCES_VALIDATED)
    await cluster_datamanager.update_kb_shards(kbid, rolled_over_shards)

    return repaired_resources


async def clean_indexed_data(app_context: ApplicationContext, kbid: str) -> None:
    rollover_datamanager = RolloverDataManager(app_context.kv_driver)
    batch = []
    async for key in rollover_datamanager.iter_indexed_keys(kbid):
        batch.append(key)
        if len(batch) >= 100:
            await rollover_datamanager.remove_indexed(kbid, batch)
            batch = []
    if len(batch) >= 0:
        await rollover_datamanager.remove_indexed(kbid, batch)


async def clean_rollover_status(app_context: ApplicationContext, kbid: str) -> None:
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)
    kb_shards = await cluster_datamanager.get_kb_shards(kbid)
    if kb_shards is None:
        logger.warning(
            "No shards found for KB, skipping clean rollover status",
            extra={"kbid": kbid},
        )
        return

    _clear_rollover_status(kb_shards)
    await cluster_datamanager.update_kb_shards(kbid, kb_shards)


async def rollover_kb_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Rollover a shard is the process of creating new shard replicas for every
    shard and indexing all existing resources into the replicas.

    Once all the data is in the new shards, cut over the registered replicas
    to the new shards and delete the old shards.

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
        logger.warning("Waiting for index nodes to be available")
        await asyncio.sleep(1)
        node_ready_checks += 1

    logger.warning("Rolling over shards", extra={"kbid": kbid})

    await create_rollover_shards(app_context, kbid)
    await schedule_resource_indexing(app_context, kbid)
    await index_rollover_shards(app_context, kbid)
    await cutover_shards(app_context, kbid)
    # we need to cut over BEFORE we validate the data
    await validate_indexed_data(app_context, kbid)
    await clean_indexed_data(app_context, kbid)
    await clean_rollover_status(app_context, kbid)

    logger.warning("Finished rolling over shards", extra={"kbid": kbid})


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
