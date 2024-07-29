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

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.context import ApplicationContext
from nucliadb_protos import nodereader_pb2, noderesources_pb2
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils import const
from nucliadb_utils.fastapi.run import serve_metrics
from nucliadb_utils.utilities import has_feature

from .settings import settings
from .utils import delete_resource_from_shard, index_resource_to_shard, wait_for_node

logger = logging.getLogger(__name__)

REBALANCE_LOCK = "rebalance"


async def get_shards_paragraphs(kbid: str) -> list[tuple[str, int]]:
    """
    Ordered shard -> num paragraph by number of paragraphs
    """
    async with datamanagers.with_ro_transaction() as txn:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    if kb_shards is None:
        return []

    results = {}
    for shard_meta in kb_shards.shards:
        node, shard_id = choose_node(shard_meta)
        shard_data: nodereader_pb2.Shard = await node.reader.GetShard(
            nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=shard_id))  # type: ignore
        )
        results[shard_meta.shard] = shard_data.paragraphs

    return [(shard, paragraphs) for shard, paragraphs in sorted(results.items(), key=lambda x: x[1])]


async def maybe_add_shard(kbid: str) -> None:
    async with locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)):
        async with datamanagers.with_ro_transaction() as txn:
            kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if kb_shards is None:
            return

        shard_paragraphs = await get_shards_paragraphs(kbid)
        total_paragraphs = sum([c for _, c in shard_paragraphs])

        if (total_paragraphs / len(kb_shards.shards)) > (
            settings.max_shard_paragraphs * 0.9  # 90% of the max
        ):
            # create new shard
            async with datamanagers.with_transaction() as txn:
                sm = get_shard_manager()
                await sm.create_shard_by_kbid(txn, kbid)
                await txn.commit()


async def move_set_of_kb_resources(
    context: ApplicationContext,
    kbid: str,
    from_shard_id: str,
    to_shard_id: str,
    count: int = 20,
) -> None:
    async with datamanagers.with_ro_transaction() as txn:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
    if kb_shards is None:  # pragma: no cover
        logger.warning("No shards found for kb. This should not happen.", extra={"kbid": kbid})
        return

    logger.info(
        "Rebalancing kb shards",
        extra={"kbid": kbid, "from": from_shard_id, "to": to_shard_id, "count": count},
    )

    from_shard = [s for s in kb_shards.shards if s.shard == from_shard_id][0]
    to_shard = [s for s in kb_shards.shards if s.shard == to_shard_id][0]

    from_node, from_shard_replica_id = choose_node(from_shard)
    search_response: nodereader_pb2.SearchResponse = await from_node.reader.Search(  # type: ignore
        nodereader_pb2.SearchRequest(
            shard=from_shard_replica_id,
            paragraph=False,
            document=True,
            result_per_page=count,
            fields=["a/title"],
        )
    )

    for result in search_response.document.results:
        resource_id = result.uuid
        try:
            async with (
                datamanagers.with_transaction() as txn,
                locking.distributed_lock(
                    locking.RESOURCE_INDEX_LOCK.format(kbid=kbid, resource_id=resource_id)
                ),
            ):
                found_shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=kbid, rid=resource_id, for_update=True
                )
                if found_shard_id is None:
                    # resource deleted
                    continue
                if found_shard_id != from_shard_id:
                    # resource could have already been moved
                    continue

                await datamanagers.resources.set_resource_shard_id(
                    txn, kbid=kbid, rid=resource_id, shard=to_shard_id
                )
                await index_resource_to_shard(context, kbid, resource_id, to_shard)
                await delete_resource_from_shard(context, kbid, resource_id, from_shard)
                await txn.commit()
        except Exception:
            logger.exception(
                "Failed to move resource",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
            # XXX Not ideal failure situation here. Try reverting the whole move even though it could be redundant
            try:
                await index_resource_to_shard(context, kbid, resource_id, from_shard)
                await delete_resource_from_shard(context, kbid, resource_id, to_shard)
            except Exception:
                logger.exception(
                    "Failed to revert move resource. Hopefully you never see this message.",
                    extra={"kbid": kbid, "resource_id": resource_id},
                )

    node_ids = set()
    for replica in from_shard.replicas:
        node_ids.add(replica.node)
    for replica in to_shard.replicas:
        node_ids.add(replica.node)
    for node_id in node_ids:
        await wait_for_node(context, node_id)


async def rebalance_kb(context: ApplicationContext, kbid: str) -> None:
    if not has_feature(const.Features.REBALANCE_KB, context={"kbid": kbid}):
        return

    await maybe_add_shard(kbid)

    shard_paragraphs = await get_shards_paragraphs(kbid)
    rebalanced_shards = set()
    while any(paragraphs > settings.max_shard_paragraphs for _, paragraphs in shard_paragraphs):
        # find the shard with the least/most paragraphs
        smallest_shard = shard_paragraphs[0][0]
        largest_shard = shard_paragraphs[-1][0]
        assert smallest_shard != largest_shard

        if smallest_shard in rebalanced_shards:
            # XXX This is to prevent flapping data between shards on a single pass
            # if we already rebalanced this shard, then we can't do anything else
            break

        await move_set_of_kb_resources(context, kbid, largest_shard, smallest_shard)

        rebalanced_shards.add(largest_shard)

        shard_paragraphs = await get_shards_paragraphs(kbid)


async def run(context: ApplicationContext) -> None:
    try:
        async with locking.distributed_lock(REBALANCE_LOCK):
            # get all kb ids
            async with datamanagers.with_ro_transaction() as txn:
                kbids = [kbid async for kbid, _ in datamanagers.kb.get_kbs(txn)]
            # go through each kb and see if shards need to be reduced in size
            for kbid in kbids:
                async with locking.distributed_lock(locking.KB_SHARDS_LOCK.format(kbid=kbid)):
                    await rebalance_kb(context, kbid)
    except locking.ResourceLocked as exc:
        if exc.key == REBALANCE_LOCK:
            logger.warning("Another rebalance process is already running.")
            return
        raise


async def run_command(context: ApplicationContext) -> None:
    setup_logging()
    await setup_telemetry("rebalancer")

    context = ApplicationContext("rebalancer")
    await context.initialize()
    metrics_server = await serve_metrics()

    try:
        await run(context)
    except (asyncio.CancelledError, RuntimeError):  # pragma: no cover
        return
    except Exception as ex:  # pragma: no cover
        logger.exception("Failed to run rebalancing.")
        errors.capture_exception(ex)
    finally:
        try:
            await metrics_server.shutdown()
            await context.finalize()
        except Exception:  # pragma: no cover
            logger.exception("Error tearing down utilities on rebalance command")
            pass


def main():
    context = ApplicationContext()
    asyncio.run(run_command(context))
