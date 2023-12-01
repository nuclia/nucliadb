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
import logging
from datetime import datetime
from typing import Optional

import backoff

from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.cluster import ClusterDataManager
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb_protos import noderesources_pb2, writer_pb2
from nucliadb_telemetry import errors
from nucliadb_utils import const

from .exceptions import ExhaustedNodesError
from .manager import get_index_node
from .settings import settings

logger = logging.getLogger(__name__)


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

    existing_rollover_shards = await cluster_datamanager.get_kb_rollover_shards(kbid)
    if existing_rollover_shards is not None:
        logger.warning(
            "Rollover shards already exist, deleting them and restarting",
            extra={"kbid": kbid},
        )
        for shard in existing_rollover_shards.shards:
            await sm.rollback_shard(shard)

    kb_shards = await cluster_datamanager.get_kb_shards(kbid)
    if kb_shards is None:
        raise UnexpectedRolloverError(f"No shards found for KB {kbid}")

    # create new shards
    created_shards = []
    try:
        for shard in kb_shards.shards:
            nodes = cluster_manager.sorted_nodes()
            shard.ClearField("replicas")
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

    await cluster_datamanager.update_kb_rollover_shards(kbid, kb_shards)
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
            return
        logger.info("Waiting for node to consume messages", extra={"node": node_id})
        await asyncio.sleep(2)


@backoff.on_exception(backoff.expo, (Exception,), max_tries=3)
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


async def index_rollover_shards(
    app_context: ApplicationContext, kbid: str
) -> dict[str, tuple[str, datetime]]:
    """
    Indexes all data in a kb in rollover shards
    """
    logger.warning("Indexing rollover shards", extra={"kbid": kbid})

    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)

    rollover_shards = await cluster_datamanager.get_kb_rollover_shards(kbid)
    if rollover_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    indexed_resources = {}
    wait_index_batch: list[writer_pb2.ShardObject] = []
    # now index on all new shards only
    async for resource_id in resources_datamanager.iterate_resource_ids(kbid):
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
            continue

        indexed_resources[resource_id] = (
            shard_id,
            resource_index_message.metadata.modified.ToDatetime(),
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

    return indexed_resources


async def cutover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Swaps our the current active shards for a knowledgebox.
    """
    logger.warning("Cutting over shards", extra={"kbid": kbid})
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)
    sm = app_context.shard_manager

    active_shards = await cluster_datamanager.get_kb_shards(kbid)
    rollover_shards = await cluster_datamanager.get_kb_rollover_shards(kbid)
    if active_shards is None or rollover_shards is None:
        raise UnexpectedRolloverError("Shards for kb not found")

    await cluster_datamanager.update_kb_shards(kbid, rollover_shards)
    await cluster_datamanager.delete_kb_rollover_shards(kbid)

    for shard in active_shards.shards:
        await sm.rollback_shard(shard)


async def validate_indexed_data(
    app_context: ApplicationContext,
    kbid: str,
    indexed_resources: dict[str, tuple[str, datetime]],
) -> list[str]:
    """
    Goes through all the resources in a knowledgebox and validates it
    against the data that was indexed during the rollover.

    If any resource is missing, it will be indexed again.

    If a resource was removed during the rollover, it will be removed as well.
    """
    logger.warning("Validating indexed data", extra={"kbid": kbid})
    sm = app_context.shard_manager
    partitioning = app_context.partitioning

    resources_datamanager = ResourcesDataManager(
        app_context.kv_driver, app_context.blob_storage
    )
    cluster_datamanager = ClusterDataManager(app_context.kv_driver)

    rolled_over_shards = await cluster_datamanager.get_kb_shards(kbid)
    if rolled_over_shards is None:
        raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    repaired_resources = []
    async for resource_id in resources_datamanager.iterate_resource_ids(kbid):
        if resource_id in indexed_resources:
            shard_id, last_indexed = indexed_resources[resource_id]
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
            last_indexed = datetime.min

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

        if res.basic.modified.ToDatetime() <= last_indexed:  # type: ignore
            # resource was not affected by rollover, carry on
            del indexed_resources[resource_id]
            continue

        resource_index_message = await index_resource(
            app_context, kbid, resource_id, shard
        )
        if resource_index_message is not None:
            if resource_id in indexed_resources:
                del indexed_resources[resource_id]
            repaired_resources.append(resource_id)

    # any left overs should be deleted
    for resource_id, (shard_id, _) in indexed_resources.items():
        shard = _get_shard(rolled_over_shards, shard_id)
        if shard is None:
            raise UnexpectedRolloverError("Shard not found. This should not happen")
        logger.warning(
            "Deleting resource from index",
            extra={"kbid": kbid, "resource_id": resource_id},
        )
        partition = partitioning.generate_partition(kbid, resource_id)
        await sm.delete_resource(shard, resource_id, -1, str(partition), kbid)

    return repaired_resources


async def rollover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Rollover a shard is the process of creating new shard replicas for every
    shard and indexing all existing resources into the replicas.

    Once all the data is in the new shards, cut over the registered replicas
    to the new shards and delete the old shards.

    This is a very expensive operation and should be done with care.

    Process:
    - Create new shards
    - Index all resources into new shards
    - Cut over replicas to new shards
    - Validate that all resources are in the new shards
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
    indexed_resources = await index_rollover_shards(app_context, kbid)
    await cutover_shards(app_context, kbid)
    # we need to cut over BEFORE we validate the data
    await validate_indexed_data(app_context, kbid, indexed_resources)

    logger.warning("Finished rolling over shards", extra={"kbid": kbid})


async def _rollover_kbid_command(kbid: str) -> None:  # pragma: no cover
    app_context = ApplicationContext()
    await app_context.initialize()
    try:
        await rollover_shards(app_context, kbid)
    finally:
        await app_context.finalize()


argparser = argparse.ArgumentParser()
argparser.add_argument("--kbid", help="Knowledge base ID to rollover", required=True)


def rollover_kbid_command() -> None:  # pragma: no cover
    args = argparser.parse_args()
    asyncio.run(_rollover_kbid_command(args.kbid))
