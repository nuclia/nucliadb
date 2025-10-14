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


async def get_kb_shards(kbid: str) -> Optional[writer_pb2.Shards]:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)


async def get_shards_paragraphs(kbid: str) -> list[tuple[str, int]]:
    """
    List KB shards and their paragraph count, sorted by increasing paragraph count (smallest first)
    """
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return []
    results = {}
    for shard_meta in kb_shards.shards:
        results[shard_meta.shard] = await get_shard_paragraph_count(shard_meta.nidx_shard_id)
    return [(shard, paragraphs) for shard, paragraphs in sorted(results.items(), key=lambda x: x[1])]


async def maybe_add_shard(kbid: str) -> None:
    """
    Adds a new shard to the kb if the average count of paragraphs per shard is above 90% of the max shard paragraph.
    """
    async with locking.distributed_lock(locking.UPDATE_SHARDS_LOCK.format(kbid=kbid)):
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

    current_active_shard = kb_shards.shards[kb_shards.actual]
    to_delete = []

    for shard in list(kb_shards.shards):
        if shard == current_active_shard:
            # This is the currently active shard, so we can't delete it
            continue
        resources_in_shard = await get_shard_resources_count(shard.nidx_shard_id)
        if resources_in_shard == 0:
            to_delete.append(shard)

    if to_delete:
        async with locking.distributed_lock(locking.UPDATE_SHARDS_LOCK.format(kbid=kbid)):
            # Delete shards from nidx
            for shard in to_delete:
                logger.info(
                    "Deleting empty shard",
                    extra={"kbid": kbid, "shard_id": shard.shard, "nidx_shard_id": shard.nidx_shard_id},
                )
                await get_nidx_api_client().DeleteShard(
                    noderesources_pb2.ShardId(id=shard.nidx_shard_id)
                )
                kb_shards.shards.remove(shard)
                kb_shards.actual -= 1
                assert kb_shards.actual >= 0

            async with datamanagers.with_rw_transaction() as txn:
                await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=kb_shards)
                await txn.commit()


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
    logger.info("Rebalance started", extra={"kbid": kbid})

    await maybe_add_shard(kbid)
    split = await split_big_shards(context, kbid)

    if not split:
        await remove_empty_shards(kbid)
        await merge_small_shards(context, kbid)

    logger.info("Rebalance finished", extra={"kbid": kbid})


async def split_big_shards(context: ApplicationContext, kbid: str) -> bool:
    """
    Checks for shards that have exceeded the max number of paragraphs by 10%
    and moves resources to other shards that have space.

    Returns whether some shards have been split.
    """
    shard_paragraphs = await get_shards_paragraphs(kbid)
    rebalanced_shards = set()
    while any(paragraphs > (settings.max_shard_paragraphs * 1.1) for _, paragraphs in shard_paragraphs):
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

    return len(rebalanced_shards) > 0


@dataclasses.dataclass
class RebalanceShard:
    id: str
    nidx_id: str
    active: bool
    paragraphs: int

    def to_dict(self):
        return self.__dict__


async def get_rebalance_shards(kbid: str) -> list[RebalanceShard]:
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return []

    def get_nidx_shard_id(sid):
        return next(s.nidx_shard_id for s in kb_shards.shards if s.shard == sid)

    current_active_shard_id = kb_shards.shards[kb_shards.actual].shard

    return [
        RebalanceShard(
            id=shard_id,
            nidx_id=get_nidx_shard_id(shard_id),
            active=(shard_id == current_active_shard_id),
            paragraphs=paragraphs,
        )
        for shard_id, paragraphs in await get_shards_paragraphs(kbid)
    ]


class NoMergeCandidatesFound(Exception):
    pass


def choose_merge_shards(candidates: list[RebalanceShard]) -> tuple[RebalanceShard, RebalanceShard]:
    if len(candidates) < 2:
        raise NoMergeCandidatesFound("not enough candidates")

    # Take the smallest empty shard as source
    def is_almost_empty_shard(s: RebalanceShard):
        return (0 < s.paragraphs < settings.max_shard_paragraphs * 0.3) and not s.active

    empty_shards = list(filter(is_almost_empty_shard, candidates))
    if not empty_shards:
        raise NoMergeCandidatesFound("no empty candidates found")
    empty_shards.sort(key=lambda x: x.paragraphs)
    source = empty_shards[0]

    # We don't want to move resources out of the currenlty active shard
    assert not source.active

    # If current active shard has room, prioritize it. Otherwise, take the biggest shard with room.
    def has_room(s: RebalanceShard):
        return (s.paragraphs + source.paragraphs) < (
            settings.max_shard_paragraphs * 0.95
        ) and s != source

    active_shard = next(c for c in candidates if c.active)
    if has_room(active_shard):
        target = active_shard
    else:
        with_room = list(filter(has_room, candidates))
        if not with_room:
            raise NoMergeCandidatesFound("no candidates with room found")
        with_room.sort(key=lambda x: x.paragraphs)
        target = with_room[-1]

    # This is not possible, but just to be double sure
    assert target != source

    return source, target


async def merge_small_shards(context: ApplicationContext, kbid: str):
    """
    Looks for read-only shards that are almost empty and could be merged into a bigger shard
    """
    emptied_shards = set()
    while True:
        rebalance_shards = await get_rebalance_shards(kbid)
        rebalance_shards = list(filter(lambda x: x.id not in emptied_shards, rebalance_shards))
        try:
            source_shard, target_shard = choose_merge_shards(rebalance_shards)
        except NoMergeCandidatesFound:
            break

        logger.info(
            "Merge candidates found!",
            extra={
                "kbid": kbid,
                "empty_shard": source_shard.to_dict(),
                "shard_with_room": target_shard.to_dict(),
            },
        )

        while True:
            remaining = await get_shard_resources_count(source_shard.nidx_id)
            if remaining == 0:
                logger.info("Shard is empty! Finishing", extra={"kbid": kbid, "shard": source_shard.id})
                emptied_shards.add(source_shard.id)
                break

            target_paragraphs = next(
                p for s, p in await get_shards_paragraphs(kbid) if s == target_shard.id
            )
            if target_paragraphs > settings.max_shard_paragraphs * 0.9:
                logger.info(
                    "Shard with room is getting full",
                    extra={"kbid": kbid, "shard": target_shard.id, "paragraphs": target_paragraphs},
                )
                break

            await move_set_of_kb_resources(context, kbid, source_shard.id, target_shard.id)


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
