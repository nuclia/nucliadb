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
from typing import Optional

from nidx_protos import nodereader_pb2, noderesources_pb2

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.nidx import get_nidx_api_client, get_nidx_searcher_client
from nucliadb_protos import writer_pb2
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils.fastapi.run import serve_metrics

from .settings import settings
from .utils import delete_resource_from_shard, index_resource_to_shard

logger = logging.getLogger(__name__)

REBALANCE_LOCK = "rebalance"


async def get_kb_shards(kbid: str) -> Optional[writer_pb2.Shards]:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)


async def get_shards_paragraphs(kbid: str) -> list[tuple[str, int]]:
    """
    Ordered shard -> num paragraph by number of paragraphs. Ordered in increasing paragraph count (smallest first).
    """
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return []

    results = {}
    for shard_meta in kb_shards.shards:
        shard_data: nodereader_pb2.Shard = await get_nidx_api_client().GetShard(
            nodereader_pb2.GetShardRequest(
                shard_id=noderesources_pb2.ShardId(id=shard_meta.nidx_shard_id)
            )  # type: ignore
        )
        results[shard_meta.shard] = shard_data.paragraphs

    return [(shard, paragraphs) for shard, paragraphs in sorted(results.items(), key=lambda x: x[1])]


async def maybe_add_shard(kbid: str) -> None:
    """
    Adds a new shard to the kb if the average count of paragraphs per shard is above 90% of the max shard paragraph.
    """
    async with locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)):
        kb_shards = await get_kb_shards(kbid)
        if kb_shards is None:
            return

        shard_paragraphs = await get_shards_paragraphs(kbid)
        total_paragraphs = sum([c for _, c in shard_paragraphs])

        if (total_paragraphs / len(kb_shards.shards)) > (
            settings.max_shard_paragraphs * 0.9  # 90% of the max
        ):
            logger.info(
                "Adding a new shard to KB, as all other shards are almost full",
                extra={
                    "kbid": kbid,
                },
            )
            # This makes new resources be assigned to the new shard
            async with datamanagers.with_rw_transaction() as txn:
                sm = get_shard_manager()
                await sm.create_shard_by_kbid(txn, kbid)
                await txn.commit()


async def remove_empty_shards(kbid: str) -> None:
    """
    Removes shards that are empty and can be removed
    """
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return

    for idx, shard in enumerate(kb_shards.shards):
        if idx == kb_shards.actual:
            # This is the currently active shard, so we can't delete it
            continue

        resources_in_shard = await get_shard_resources_count(shard.nidx_shard_id)
        if resources_in_shard == 0:
            logger.info(
                "Deleting empty shard",
                extra={"kbid": kbid, "shard_id": shard.shard, "nidx_shard_id": shard.nidx_shard_id},
            )
            await get_nidx_api_client().DeleteShard(noderesources_pb2.ShardId(id=shard.nidx_shard_id))
            async with datamanagers.with_rw_transaction() as txn:
                kb_shards.shards.remove(shard)
                kb_shards.actual -= 1
                await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)


async def get_shard_resources_count(nidx_shard_id: str) -> int:
    shard_data: nodereader_pb2.Shard = await get_nidx_api_client().GetShard(
        nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=nidx_shard_id))  # type: ignore
    )
    return shard_data.fields


async def move_set_of_kb_resources(
    context: ApplicationContext,
    kbid: str,
    from_shard_id: str,
    to_shard_id: str,
    count: int = 20,
) -> None:
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:  # pragma: no cover
        logger.warning("No shards found for kb. This should not happen.", extra={"kbid": kbid})
        return

    logger.info(
        "Rebalancing kb shards",
        extra={"kbid": kbid, "from": from_shard_id, "to": to_shard_id, "count": count},
    )

    from_shard = [s for s in kb_shards.shards if s.shard == from_shard_id][0]
    to_shard = [s for s in kb_shards.shards if s.shard == to_shard_id][0]

    # Do a search on the fields (fulltext) index by title so you get unique resource ids
    request = nodereader_pb2.SearchRequest(
        shard=from_shard.nidx_shard_id,
        paragraph=False,
        document=True,
        result_per_page=count,
    )
    request.field_filter.field.field_type = "a"
    request.field_filter.field.field_id = "title"
    search_response: nodereader_pb2.SearchResponse = await get_nidx_searcher_client().Search(request)

    for result in search_response.document.results:
        resource_id = result.uuid
        try:
            async with (
                datamanagers.with_rw_transaction() as txn,
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


async def rebalance_kb(context: ApplicationContext, kbid: str) -> None:
    await maybe_add_shard(kbid)
    await remove_empty_shards(kbid)

    await split_shards(context, kbid)
    await consolidate_shards(context, kbid)


async def split_shards(context: ApplicationContext, kbid: str):
    """
    Checks for shards that have exceeded the max number of paragraphs
    and moves resources to other shards that have space.
    """
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

        logger.info(
            "Shard with more resources than max found!",
            extra={"kbid": kbid, "largest_shard": largest_shard, "smallest_shard": smallest_shard},
        )
        await move_set_of_kb_resources(context, kbid, largest_shard, smallest_shard)

        rebalanced_shards.add(largest_shard)

        shard_paragraphs = await get_shards_paragraphs(kbid)


async def consolidate_shards(context: ApplicationContext, kbid: str):
    """
    Looks for read-only shards that are almost empty and could be merged into a bigger shard
    """
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return

    current_active_shard = kb_shards.shards[kb_shards.actual]
    shard_paragraphs = await get_shards_paragraphs(kbid)
    almost_empty_shards = [
        s
        for s, p in shard_paragraphs
        if p < settings.max_shard_paragraphs * 0.3 and s != current_active_shard.shard
    ]
    shards_with_room_for_more = [
        s
        for s, p in shard_paragraphs
        if s not in almost_empty_shards and p < settings.max_shard_paragraphs * 0.6
    ]

    if almost_empty_shards and shards_with_room_for_more:
        empty_shard = almost_empty_shards[0]
        shard_with_room = shards_with_room_for_more[0]
        assert empty_shard != shard_with_room

        logger.info(
            "Empty shard found!",
            extra={"kbid": kbid, "empty_shard": empty_shard, "shard_with_room": shard_with_room},
        )

        from_shard = [s for s in kb_shards.shards if s.shard == empty_shard][0]
        to_shard = [s for s in kb_shards.shards if s.shard == shard_with_room][0]

        while True:
            empty_shard_remaining_resources = await get_shard_resources_count(from_shard.nidx_shard_id)
            if empty_shard_remaining_resources == 0:
                logger.info("Shard is empty! Finishing", extra={"kbid": kbid, "shard": empty_shard})
                break

            shard_paragraphs = await get_shards_paragraphs(kbid)
            with_room_paragraphs = next(p for s, p in shard_paragraphs if s == shard_with_room)
            if with_room_paragraphs > settings.max_shard_paragraphs * 0.9:
                logger.info(
                    "Shard with room is getting full",
                    extra={"kbid": kbid, "shard": shard_with_room, "paragraphs": with_room_paragraphs},
                )
                break

            await move_set_of_kb_resources(context, kbid, from_shard.shard, to_shard.shard)


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
