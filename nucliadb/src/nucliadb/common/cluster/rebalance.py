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
import dataclasses
import logging
import math
from typing import Optional

from grpc import StatusCode
from grpc.aio import AioRpcError
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


@dataclasses.dataclass
class RebalanceShard:
    id: str
    nidx_id: str
    paragraphs: int

    def to_dict(self):
        return self.__dict__


async def get_kb_shards(kbid: str) -> Optional[writer_pb2.Shards]:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)


async def get_shard_resources_count(nidx_shard_id: str) -> int:
    # Do a search on the fields (fulltext) index by title so you get unique resource ids
    try:
        request = nodereader_pb2.SearchRequest(
            shard=nidx_shard_id,
            paragraph=False,
            document=True,
            result_per_page=0,
        )
        request.field_filter.field.field_type = "a"
        request.field_filter.field.field_id = "title"
        search_response: nodereader_pb2.SearchResponse = await get_nidx_searcher_client().Search(request)
        return search_response.document.total
    except AioRpcError as exc:
        if exc.code() == StatusCode.NOT_FOUND:
            logger.warning(f"Shard not found in nidx", extra={"nidx_shard_id": nidx_shard_id})
            return 0
        raise


async def get_shard_paragraph_count(nidx_shard_id: str) -> int:
    # Do a search on the fields (paragraph) index
    try:
        request = nodereader_pb2.SearchRequest(
            shard=nidx_shard_id,
            paragraph=True,
            document=False,
            result_per_page=0,
        )
        search_response: nodereader_pb2.SearchResponse = await get_nidx_searcher_client().Search(request)
        return search_response.paragraph.total
    except AioRpcError as exc:
        if exc.code() == StatusCode.NOT_FOUND:
            logger.warning(f"Shard not found in nidx", extra={"nidx_shard_id": nidx_shard_id})
            return 0
        raise


async def get_rebalance_shards(kbid: str) -> list[RebalanceShard]:
    """
    Return the sorted list of shards by increasing paragraph count. The current active shard is excluded
    from the rebalance operations, to avoid conflicts with ingestion.
    """
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return []
    return list(
        sorted(
            [
                RebalanceShard(
                    id=shard.shard,
                    nidx_id=shard.nidx_shard_id,
                    paragraphs=await get_shard_paragraph_count(shard.nidx_shard_id),
                )
                for idx, shard in enumerate(kb_shards.shards)
                if idx != kb_shards.actual  # Skip current active shard
            ],
            key=lambda x: x.paragraphs,
        )
    )


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

    from_shard = next(s for s in kb_shards.shards if s.shard == from_shard_id)
    to_shard = next(s for s in kb_shards.shards if s.shard == to_shard_id)

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


def needs_rebalance(shard: RebalanceShard, shards: list[RebalanceShard]) -> bool:
    return needs_split(shard) or needs_merge(shard, shards)


def needs_split(shard: RebalanceShard) -> bool:
    # We don't split shards unless they exceed the max by 20%
    return shard.paragraphs > (settings.max_shard_paragraphs * 1.2)


def calculate_capacity(shards: list[RebalanceShard]) -> float:
    return sum([max(0, (settings.max_shard_paragraphs - sh.paragraphs)) for sh in shards])


def needs_merge(shard: RebalanceShard, all_shards: list[RebalanceShard]) -> bool:
    # We don't consider shards for merging if they are already more that 75% of max
    if shard.paragraphs > (settings.max_shard_paragraphs * 0.75):
        return False
    # The shard can be merged as long as the cluster has capacity.
    # Take into account that we will not fill shards more than 90%
    other_shards = [s for s in all_shards if s.id != shard.id]
    return shard.paragraphs < (calculate_capacity(other_shards) * 0.9)


async def split_shard(
    context: ApplicationContext, kbid: str, shard: RebalanceShard, shards: list[RebalanceShard]
):
    logger.info(
        "Splitting excess of paragraphs to other shards",
        extra={
            "kbid": kbid,
            "shard": shard.to_dict(),
        },
    )

    # First off, calculate if the excess fits in the cluster
    excess = shard.paragraphs - settings.max_shard_paragraphs
    other_shards = [s for s in shards if s.id != shard.id]
    capacity = calculate_capacity(other_shards) * 0.9
    if excess > capacity:
        shards_to_add = math.ceil((excess - capacity) / settings.max_shard_paragraphs)
        logger.info(
            "More shards needed",
            extra={
                "kbid": kbid,
                "shards_to_add": shards_to_add,
                "all_shards": [s.to_dict() for s in shards],
            },
        )
        # Add new shards where to rebalance the excess of paragraphs
        async with (
            locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)),
            datamanagers.with_rw_transaction() as txn,
        ):
            sm = get_shard_manager()
            for _ in range(shards_to_add):
                await sm.create_shard_by_kbid(txn, kbid)
            await txn.commit()

        # Recalculate after having created shards, the active shard is a different one
        shards = await get_rebalance_shards(kbid)

    # Keep moving resoures to other shards as long as we are still over the max
    with_room = [
        s for s in shards if s.id != shard.id and s.paragraphs < settings.max_shard_paragraphs * 0.9
    ]
    shard_paragraphs = shard.paragraphs
    i = 0
    while (shard_paragraphs > settings.max_shard_paragraphs) and i < 500:
        # Get the biggest shard with some space left
        try:
            target_shard = list(sorted(with_room, key=lambda x: x.paragraphs))[-1]
        except IndexError:
            # No target shard found
            break

        # Move some resources to the new shard
        await move_set_of_kb_resources(
            context, kbid, from_shard_id=shard.id, to_shard_id=target_shard.id, count=20
        )

        # Recalculate counts
        all_shards = await get_rebalance_shards(kbid)
        shard_paragraphs = next(s.paragraphs for s in all_shards if s.id == shard.id)
        with_room = [
            s for s in shards if s.id != shard.id and s.paragraphs < settings.max_shard_paragraphs * 0.9
        ]
        i += 0


async def merge_shard(
    context: ApplicationContext, kbid: str, shard: RebalanceShard, shards: list[RebalanceShard]
):
    logger.info(
        "Merging shard",
        extra={
            "kbid": kbid,
            "shard": shard.to_dict(),
        },
    )
    empty_shard = False

    i = 0
    while i < 1000:
        resources_count = await get_shard_resources_count(shard.nidx_id)
        if resources_count == 0:
            logger.info(
                "Shard is now empty",
                extra={
                    "kbid": kbid,
                    "shard": shard.to_dict(),
                },
            )
            empty_shard = True
            break

        logger.info(
            "Shard not yet empty",
            extra={
                "kbid": kbid,
                "shard": shard.to_dict(),
                "remaining": resources_count,
            },
        )

        # Take the biggest shard that has some capacity (90% of the max, to prevent filling them completely)
        with_room = [
            s for s in shards if s.id != shard.id and s.paragraphs < settings.max_shard_paragraphs * 0.9
        ]
        with_room.sort(key=lambda x: x.paragraphs)
        try:
            target_shard = with_room[-1]
        except IndexError:
            # No target shard could be found. Move on
            break

        await move_set_of_kb_resources(
            context, kbid, from_shard_id=shard.id, to_shard_id=target_shard.id, count=20
        )

        # Recompute shards, as counts have changed
        shards = await get_rebalance_shards(kbid)
        i += 1

    if empty_shard:
        async with locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)):
            kb_shards = await get_kb_shards(kbid)
            if kb_shards is not None:
                # Delete shard from nidx
                to_delete = next(s for s in kb_shards.shards if s.shard == shard.id)
                logger.info(
                    "Deleting empty shard",
                    extra={"kbid": kbid, "shard_id": shard.id, "nidx_shard_id": shard.nidx_id},
                )
                await get_nidx_api_client().DeleteShard(
                    noderesources_pb2.ShardId(id=to_delete.nidx_shard_id)
                )
                kb_shards.shards.remove(to_delete)
                kb_shards.actual -= 1
                assert kb_shards.actual >= 0

                async with datamanagers.with_rw_transaction() as txn:
                    await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)
                    await txn.commit()


async def rebalance_kb(context: ApplicationContext, kbid: str) -> None:
    """
    Iterate over shards until none of them need more rebalancing.
    Will move excess of paragraphs to other shards (potentially creating new ones), and
    merge small shards together when possible (potentially deleting empty ones.)
    """
    shards = await get_rebalance_shards(kbid)
    while True:
        try:
            shard = next(s for s in shards if needs_rebalance(s, shards))
        except StopIteration:
            break
        if needs_split(shard):
            await split_shard(context, kbid, shard, shards)
        elif needs_merge(shard, shards):
            await merge_shard(context, kbid, shard, shards)
        shards = await get_rebalance_shards(kbid)


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
