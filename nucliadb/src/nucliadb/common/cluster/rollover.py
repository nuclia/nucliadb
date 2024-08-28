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

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster import manager as cluster_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.rollover import RolloverState, RolloverStateNotFoundError
from nucliadb.common.external_index_providers.base import ExternalIndexManager
from nucliadb.common.external_index_providers.manager import (
    get_external_index_manager,
)
from nucliadb_protos import nodewriter_pb2, writer_pb2
from nucliadb_telemetry import errors

from .manager import get_index_node
from .settings import settings
from .utils import (
    delete_resource_from_shard,
    get_resource,
    get_resource_index_message,
    index_resource_to_shard,
    wait_for_node,
)

logger = logging.getLogger(__name__)


class UnexpectedRolloverError(Exception):
    pass


async def create_rollover_index(
    app_context: ApplicationContext,
    kbid: str,
    drain_nodes: Optional[list[str]] = None,
    external: Optional[ExternalIndexManager] = None,
) -> None:
    """
    Creates a new index for a knowledgebox in the cluster and to the external index provider if configured.
    """
    await create_rollover_shards(app_context, kbid, drain_nodes=drain_nodes)

    if external is not None:
        if external.supports_rollover:
            await create_rollover_external_index(kbid, external)
        else:
            logger.info(
                "External index provider does not support rollover",
                extra={"kbid": kbid, "external_index_provider": external.type.value},
            )


async def create_rollover_external_index(kbid: str, external: ExternalIndexManager) -> None:
    extra = {"kbid": kbid, "external_index_provider": external.type.value}
    async with datamanagers.with_ro_transaction() as txn:
        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if state.external_index_created:
            logger.info("Rollover external index already created, skipping", extra=extra)
            return

    logger.info("Creating rollover external index", extra=extra)
    async with datamanagers.with_ro_transaction() as txn:
        stored_metadata = await datamanagers.kb.get_external_index_provider_metadata(txn, kbid=kbid)
        if stored_metadata is None:
            raise UnexpectedRolloverError("External index metadata not found")

    rollover_metadata = await external.rollover_create_indexes(stored_metadata)

    async with datamanagers.with_rw_transaction() as txn:
        await datamanagers.rollover.update_kb_rollover_external_index_metadata(
            txn, kbid=kbid, metadata=rollover_metadata
        )
        state.external_index_created = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
        await txn.commit()


async def create_rollover_shards(
    app_context: ApplicationContext, kbid: str, drain_nodes: Optional[list[str]] = None
) -> writer_pb2.Shards:
    """
    Creates new index node shards for a rollover operation.
    If drain_nodes is provided, no replicas will be created on those nodes.
    """

    logger.info("Creating rollover shards", extra={"kbid": kbid})
    sm = app_context.shard_manager

    async with datamanagers.with_ro_transaction() as txn:
        try:
            state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        except RolloverStateNotFoundError:
            # State is not set yet, create it
            state = RolloverState(
                rollover_shards_created=False,
                external_index_created=False,
                resources_scheduled=False,
                resources_indexed=False,
                cutover_shards=False,
                cutover_external_index=False,
                resources_validated=False,
            )

        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if kb_shards is None:
            raise UnexpectedRolloverError(f"No shards found for KB {kbid}")

        if state.rollover_shards_created:
            logger.info("Rollover shards already created, skipping", extra={"kbid": kbid})
            return kb_shards

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

                vectorsets = {
                    vectorset_id: vectorset_config.vectorset_index_config
                    async for vectorset_id, vectorset_config in datamanagers.vectorsets.iter(
                        txn, kbid=kbid
                    )
                }
                try:
                    if not vectorsets:
                        is_matryoshka = len(kb_shards.model.matryoshka_dimensions) > 0
                        vector_index_config = nodewriter_pb2.VectorIndexConfig(
                            similarity=kb_shards.similarity,
                            vector_type=nodewriter_pb2.VectorType.DENSE_F32,
                            vector_dimension=kb_shards.model.vector_dimension,
                            normalize_vectors=is_matryoshka,
                        )
                        shard_created = await node.new_shard(
                            kbid,
                            release_channel=kb_shards.release_channel,
                            vector_index_config=vector_index_config,
                        )
                    else:
                        shard_created = await node.new_shard_with_vectorsets(
                            kbid,
                            release_channel=kb_shards.release_channel,
                            vectorsets_configs=vectorsets,
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
        state.rollover_shards_created = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
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
    logger.info("Scheduling resources to be indexed to rollover shards", extra={"kbid": kbid})
    async with datamanagers.with_ro_transaction() as txn:
        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if not state.rollover_shards_created:
            raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")
        if state.resources_scheduled:
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
        state.resources_scheduled = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
        await txn.commit()


def _to_ts(dt: datetime) -> int:
    return int(dt.timestamp() * 1000 * 1000)


async def index_to_rollover_index(
    app_context: ApplicationContext, kbid: str, external: Optional[ExternalIndexManager] = None
) -> None:
    """
    Indexes all data in a kb in rollover indexes. This happens before the cutover.
    """
    extra = {"kbid": kbid, "external_index_provider": None}
    if external is not None:
        extra["external_index_provider"] = external.type.value
    async with datamanagers.with_ro_transaction() as txn:
        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if not all([state.rollover_shards_created, state.resources_scheduled]):
            raise UnexpectedRolloverError(f"Preconditions not met for KB {kbid}")
        rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
        if rollover_shards is None:
            raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")
    if state.resources_indexed:
        logger.info("Resources already indexed, skipping", extra=extra)
        return

    logger.info("Indexing to rollover index", extra=extra)
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
        resource = await get_resource(kbid, resource_id)
        index_message = await get_resource_index_message(kbid, resource_id)
        if resource is None or index_message is None:
            # resource no longer existing, remove indexing and carry on
            async with datamanagers.with_transaction() as txn:
                await datamanagers.rollover.remove_to_index(txn, kbid=kbid, resource=resource_id)
                await txn.commit()
            continue

        if external is not None:
            await external.index_resource(resource_id, index_message, to_rollover_indexes=True)
        await index_resource_to_shard(
            app_context, kbid, resource_id, shard, resource_index_message=index_message
        )

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

    async with datamanagers.with_transaction() as txn:
        state.resources_indexed = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
        await datamanagers.rollover.update_kb_rollover_shards(txn, kbid=kbid, kb_shards=rollover_shards)
        await txn.commit()


async def cutover_index(
    app_context: ApplicationContext, kbid: str, external: Optional[ExternalIndexManager] = None
) -> None:
    """
    Swaps our the current active index for a knowledgebox.
    """
    await cutover_shards(app_context, kbid)
    if external is not None:
        if external.supports_rollover:
            await cutover_external_index(kbid, external)
        else:
            logger.info(
                "External index provider does not support rollover",
                extra={"kbid": kbid, "external_index_provider": external.type.value},
            )


async def cutover_external_index(kbid: str, external: ExternalIndexManager) -> None:
    """
    Cuts over to the newly creted external index for a knowledgebox.
    The old indexes are deleted.
    """
    extra = {"kbid": kbid, "external_index_provider": external.type.value}
    logger.info("Cutting over external index", extra=extra)
    async with datamanagers.with_rw_transaction() as txn:
        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if not all(
            [
                state.rollover_shards_created,
                state.resources_scheduled,
                state.resources_indexed,
            ]
        ):
            raise UnexpectedRolloverError(f"Preconditions not met for KB {kbid}")
        if state.cutover_external_index:
            logger.info("External index already cut over, skipping", extra=extra)
            return

        stored_metadata = await datamanagers.kb.get_external_index_provider_metadata(txn, kbid=kbid)
        rollover_metadata = await datamanagers.rollover.get_kb_rollover_external_index_metadata(
            txn, kbid=kbid
        )
        if stored_metadata is None or rollover_metadata is None:
            raise UnexpectedRolloverError("stored or rollover external index metadata not found")

        await external.rollover_cutover_indexes()

        await datamanagers.kb.set_external_index_provider_metadata(
            txn, kbid=kbid, metadata=rollover_metadata
        )
        await datamanagers.rollover.delete_kb_rollover_external_index_metadata(txn, kbid=kbid)
        state.cutover_external_index = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
        await txn.commit()


async def cutover_shards(app_context: ApplicationContext, kbid: str) -> None:
    """
    Swaps our the current active shards for a knowledgebox.
    """
    logger.info("Cutting over shards", extra={"kbid": kbid})
    async with datamanagers.with_transaction() as txn:
        sm = app_context.shard_manager

        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if not all(
            [
                state.rollover_shards_created,
                state.resources_scheduled,
                state.resources_indexed,
            ]
        ):
            raise UnexpectedRolloverError(f"Preconditions not met for KB {kbid}")
        if state.cutover_shards:
            logger.info("Shards already cut over, skipping", extra={"kbid": kbid})
            return

        previously_active_shards = await datamanagers.cluster.get_kb_shards(
            txn, kbid=kbid, for_update=True
        )
        rollover_shards = await datamanagers.rollover.get_kb_rollover_shards(txn, kbid=kbid)
        if previously_active_shards is None or rollover_shards is None:
            raise UnexpectedRolloverError("Shards for kb not found")

        await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=rollover_shards)
        await datamanagers.rollover.delete_kb_rollover_shards(txn, kbid=kbid)

        for shard in previously_active_shards.shards:
            await sm.rollback_shard(shard)

        state.cutover_shards = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)

        await txn.commit()


async def validate_indexed_data(
    app_context: ApplicationContext, kbid: str, external: Optional[ExternalIndexManager] = None
) -> list[str]:
    """
    Goes through all the resources in a knowledgebox and validates it
    against the data that was indexed during the rollover.

    If any resource is missing, it will be indexed again.

    If a resource was removed during the rollover, it will be removed as well.
    """
    extra = {"kbid": kbid, "external_index_provider": None}
    if external is not None:
        extra["external_index_provider"] = external.type.value
    async with datamanagers.with_ro_transaction() as txn:
        state = await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        if not all(
            [
                state.rollover_shards_created,
                state.resources_scheduled,
                state.resources_indexed,
                state.cutover_shards,
            ]
        ):
            raise UnexpectedRolloverError(f"Preconditions not met for KB {kbid}")

        rolled_over_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if rolled_over_shards is None:
            raise UnexpectedRolloverError(f"No rollover shards found for KB {kbid}")

    if state.resources_validated:
        logger.info("Resources already validated, skipping", extra=extra)
        return []

    logger.info("Validating indexed data", extra=extra)

    repaired_resources: list[str] = []
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
                    extra={"resource_id": resource_id, **extra},
                )
                raise UnexpectedRolloverError("Shard id not found for resource")
            last_indexed = 0

        shard = _get_shard(rolled_over_shards, shard_id)
        if shard is None:
            logger.error(
                "Shard not found for resource",
                extra={
                    "resource_id": resource_id,
                    "shard_id": shard_id,
                    **extra,
                },
            )
            raise UnexpectedRolloverError(f"Shard {shard_id} not found. This should not happen")

        res = await get_resource(kbid, resource_id)
        if res is None:
            logger.error(
                "Resource not found while validating, skipping",
                extra={"resource_id": resource_id, **extra},
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

        index_message = await get_resource_index_message(kbid, resource_id)
        if index_message is None:
            logger.error(
                "Resource index message not found while validating, skipping",
                extra={"resource_id": resource_id, **extra},
            )
            continue

        # resource was modified or added during rollover, reindex
        if external is not None:
            await external.index_resource(
                resource_id,
                index_message,
                to_rollover_indexes=True,
            )
        await index_resource_to_shard(
            app_context, kbid, resource_id, shard, resource_index_message=index_message
        )
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

    async with datamanagers.with_transaction() as txn:
        state.resources_validated = True
        await datamanagers.rollover.set_rollover_state(txn, kbid=kbid, state=state)
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
        try:
            await datamanagers.rollover.get_rollover_state(txn, kbid=kbid)
        except RolloverStateNotFoundError:
            logger.warning(
                "No rollover state found, skipping clean rollover status", extra={"kbid": kbid}
            )
            return
        await datamanagers.rollover.clear_rollover_state(txn, kbid=kbid)
        await txn.commit()


async def wait_for_cluster_ready() -> None:
    node_ready_checks = 0
    while len(cluster_manager.INDEX_NODES) == 0:
        if node_ready_checks > 10:
            raise Exception("No index nodes available")
        logger.info("Waiting for index nodes to be available")
        await asyncio.sleep(1)
        node_ready_checks += 1


async def rollover_kb_index(
    app_context: ApplicationContext, kbid: str, drain_nodes: Optional[list[str]] = None
) -> None:
    """
    Rollover a KB index is the process of creating new shard replicas for every
    shard and indexing all existing resources into the replicas. Also includes creating new external indexes if
    the KB is configured to use them.

    Once all the data is in the new indexes, cut over to the replicated index delete the old one.

    If drain_nodes is provided, no index node replicas will be created on those nodes. This is useful
    for when we want to remove a set of nodes from the index node cluster.

    This is a very expensive operation and should be done with care.

    Process:
    - Create new index for kb index (index node shards and external indexes, if configured)
    - Schedule all resources to be indexed
    - Index all resources into new kb index (index node shards and external indexes, if configured)
    - Cut over replicas to new shards (and external indexes if configured)
    - Validate that all resources are in the new kb index
    - Clean up indexed data
    """
    await wait_for_cluster_ready()

    extra = {"kbid": kbid, "external_index_provider": None}
    external = await get_external_index_manager(kbid, for_rollover=True)
    if external is not None:
        extra["external_index_provider"] = external.type.value
    logger.info("Rolling over KB index", extra=extra)

    async with locking.distributed_lock(locking.KB_SHARDS_LOCK.format(kbid=kbid)):
        await create_rollover_index(app_context, kbid, drain_nodes=drain_nodes, external=external)
        await schedule_resource_indexing(app_context, kbid)
        await index_to_rollover_index(app_context, kbid, external=external)
        await cutover_index(app_context, kbid, external=external)
        # we need to cut over BEFORE we validate the data
        await validate_indexed_data(app_context, kbid, external=external)
        await clean_indexed_data(app_context, kbid)
        await clean_rollover_status(app_context, kbid)

    logger.info("Finished rolling over KB indes", extra=extra)


async def _rollover_kbid_command(kbid: str) -> None:  # pragma: no cover
    app_context = ApplicationContext()
    await app_context.initialize()
    try:
        await rollover_kb_index(app_context, kbid)
    finally:
        await app_context.finalize()


argparser = argparse.ArgumentParser()
argparser.add_argument("--kbid", help="Knowledge base ID to rollover", required=True)


def rollover_kbid_command() -> None:  # pragma: no cover
    args = argparser.parse_args()
    asyncio.run(_rollover_kbid_command(args.kbid))
