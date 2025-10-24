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
import random
from typing import Optional

import aioitertools
from grpc import StatusCode
from grpc.aio import AioRpcError
from nidx_protos import nodereader_pb2, noderesources_pb2

from nucliadb.common import datamanagers, locking
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers.resources import KB_RESOURCE_SHARD
from nucliadb.common.nidx import get_nidx_api_client, get_nidx_searcher_client
from nucliadb_protos import writer_pb2
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.utils import setup_telemetry
from nucliadb_utils import const
from nucliadb_utils.fastapi.run import serve_metrics
from nucliadb_utils.utilities import has_feature

from .settings import settings
from .utils import delete_resource_from_shard, index_resource_to_shard, wait_for_nidx

logger = logging.getLogger(__name__)

REBALANCE_LOCK = "rebalance"

MAX_MOVES_PER_SHARD = 100


@dataclasses.dataclass
class RebalanceShard:
    id: str
    nidx_id: str
    paragraphs: int
    active: bool

    def to_dict(self):
        return self.__dict__


class Rebalancer:
    def __init__(self, context: ApplicationContext, kbid: str):
        self.context = context
        self.kbid = kbid
        self.kb_shards: Optional[writer_pb2.Shards] = None
        self.index: dict[str, set[str]] = {}

    async def get_rebalance_shards(self) -> list[RebalanceShard]:
        """
        Return the sorted list of shards by increasing paragraph count.
        """
        self.kb_shards = await datamanagers.atomic.cluster.get_kb_shards(kbid=self.kbid)
        if self.kb_shards is None:  # pragma: no cover
            return []
        return list(
            sorted(
                [
                    RebalanceShard(
                        id=shard.shard,
                        nidx_id=shard.nidx_shard_id,
                        paragraphs=await get_shard_paragraph_count(shard.nidx_shard_id),
                        active=(idx == self.kb_shards.actual),
                    )
                    for idx, shard in enumerate(self.kb_shards.shards)
                ],
                key=lambda x: x.paragraphs,
            )
        )

    async def build_shard_resources_index(self):
        async with datamanagers.with_ro_transaction() as txn:
            iterable = datamanagers.resources.iterate_resource_ids(kbid=self.kbid)
            async for resources_batch in aioitertools.batched(iterable, n=200):
                shards = await txn.batch_get(
                    keys=[KB_RESOURCE_SHARD.format(kbid=self.kbid, uuid=rid) for rid in resources_batch],
                    for_update=False,
                )
                for rid, shard_bytes in zip(resources_batch, shards):
                    if shard_bytes is not None:
                        self.index.setdefault(shard_bytes.decode(), set()).add(rid)

    async def move_paragraphs(
        self, from_shard: RebalanceShard, to_shard: RebalanceShard, max_paragraphs: int
    ) -> int:
        """
        Takes random resources from the source shard and tries to move at most max_paragraphs.
        It stops moving paragraphs until the are no more resources to move.
        """
        moved_paragraphs = 0

        while moved_paragraphs < max_paragraphs:
            # Take a random resource to move
            try:
                resource_id = random.choice(tuple(self.index[from_shard.id]))
            except (KeyError, IndexError):
                # No more resources in shard or shard not found
                break

            assert self.kb_shards is not None
            from_shard_obj = next(s for s in self.kb_shards.shards if s.shard == from_shard.id)
            to_shard_obj = next(s for s in self.kb_shards.shards if s.shard == to_shard.id)
            paragraphs_count = await get_resource_paragraphs_count(resource_id, from_shard.nidx_id)
            moved = await move_resource_to_shard(
                self.context, self.kbid, resource_id, from_shard_obj, to_shard_obj
            )
            if moved:
                self.index[from_shard.id].remove(resource_id)
                self.index.setdefault(to_shard.id, set()).add(resource_id)
                moved_paragraphs += paragraphs_count

        return moved_paragraphs

    async def wait_for_indexing(self):
        try:
            self.context.nats_manager
        except AssertionError:  # pragma: no cover
            logger.warning(f"Nats manager not initialized. Cannot wait for indexing")
            return
        while True:
            try:
                await wait_for_nidx(self.context.nats_manager, max_wait_seconds=60, max_pending=1000)
                return
            except asyncio.TimeoutError:
                logger.warning("Nidx is behind. Backing off rebalancing.", extra={"kbid": self.kbid})
                await asyncio.sleep(30)

    async def rebalance_shards(self):
        """
        Iterate over shards until none of them need more rebalancing.

        Will move excess of paragraphs to other shards (potentially creating new ones), and
        merge small shards together when possible (potentially deleting empty ones.)


        Merge chooses a <90% filled shard and fills it to almost 100%
        Split chooses a >110% filled shard and reduces it to 100%
        If the shard is between 90% and 110% full, nobody touches it
        """
        logger.info("Starting rebalance for kb", extra={"kbid", self.kbid})
        await self.build_shard_resources_index()
        while True:
            await self.wait_for_indexing()

            shards = await self.get_rebalance_shards()

            # Any shards to split?
            shard_to_split = next((s for s in shards[::-1] if needs_split(s)), None)
            if shard_to_split is not None:
                await self.split_shard(shard_to_split, shards)
                continue

            # Any shards to merge?
            shard_to_merge = next((s for s in shards if needs_merge(s, shards)), None)
            if shard_to_merge is not None:
                await self.merge_shard(shard_to_merge, shards)
            else:
                break
        logger.info("Finished rebalance for kb", extra={"kbid", self.kbid})

    async def split_shard(self, shard_to_split: RebalanceShard, shards: list[RebalanceShard]):
        logger.info(
            "Splitting excess of paragraphs to other shards",
            extra={
                "kbid": self.kbid,
                "shard": shard_to_split.to_dict(),
            },
        )

        # First off, calculate if the excess fits in the other shards or we need to add a new shard.
        # Note that we don't filter out the active shard on purpose.
        excess = shard_to_split.paragraphs - settings.max_shard_paragraphs
        other_shards = [s for s in shards if s.id != shard_to_split.id]
        other_shards_capacity = sum(
            [max(0, (settings.max_shard_paragraphs - s.paragraphs)) for s in other_shards]
        )
        if excess > other_shards_capacity:
            shards_to_add = math.ceil((excess - other_shards_capacity) / settings.max_shard_paragraphs)
            logger.info(
                "More shards needed",
                extra={
                    "kbid": self.kbid,
                    "shards_to_add": shards_to_add,
                    "all_shards": [s.to_dict() for s in shards],
                },
            )
            # Add new shards where to rebalance the excess of paragraphs
            async with (
                locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=self.kbid)),
                datamanagers.with_rw_transaction() as txn,
            ):
                kb_config = await datamanagers.kb.get_config(txn, kbid=self.kbid)
                prewarm = kb_config is not None and kb_config.prewarm_enabled
                sm = get_shard_manager()
                for _ in range(shards_to_add):
                    await sm.create_shard_by_kbid(txn, self.kbid, prewarm_enabled=prewarm)
                await txn.commit()

            # Recalculate after having created shards, the active shard is a different one
            shards = await self.get_rebalance_shards()

        # Now, move resources to other shards as long as we are still over the max
        for _ in range(MAX_MOVES_PER_SHARD):
            shard_paragraphs = next(s.paragraphs for s in shards if s.id == shard_to_split.id)
            excess = shard_paragraphs - settings.max_shard_paragraphs
            if excess <= 0:
                logger.info(
                    "Shard rebalanced successfuly",
                    extra={"kbid": self.kbid, "shard": shard_to_split.to_dict()},
                )
                break

            target_shard, target_capacity = get_target_shard(shards, shard_to_split, skip_active=False)
            if target_shard is None:
                logger.warning("No target shard found for splitting", extra={"kbid": self.kbid})
                break

            moved_paragraphs = await self.move_paragraphs(
                from_shard=shard_to_split,
                to_shard=target_shard,
                max_paragraphs=min(excess, target_capacity),
            )

            # Update shard paragraph counts
            shard_to_split.paragraphs -= moved_paragraphs
            target_shard.paragraphs += moved_paragraphs
            shards.sort(key=lambda x: x.paragraphs)

            await self.wait_for_indexing()

    async def merge_shard(self, shard_to_merge: RebalanceShard, shards: list[RebalanceShard]):
        logger.info(
            "Merging shard",
            extra={
                "kbid": self.kbid,
                "shard": shard_to_merge.to_dict(),
            },
        )
        empty_shard = False

        for _ in range(MAX_MOVES_PER_SHARD):
            resources_count = len(self.index.get(shard_to_merge.id, []))
            if resources_count == 0:
                logger.info(
                    "Shard is now empty",
                    extra={
                        "kbid": self.kbid,
                        "shard": shard_to_merge.to_dict(),
                    },
                )
                empty_shard = True
                break

            logger.info(
                "Shard not yet empty",
                extra={
                    "kbid": self.kbid,
                    "shard": shard_to_merge.to_dict(),
                    "remaining": resources_count,
                },
            )

            target_shard, target_capacity = get_target_shard(shards, shard_to_merge, skip_active=True)
            if target_shard is None:
                logger.warning(
                    "No target shard could be found for merging. Moving on",
                    extra={"kbid": self.kbid, "shard": shard_to_merge.to_dict()},
                )
                break

            moved_paragraphs = await self.move_paragraphs(
                from_shard=shard_to_merge,
                to_shard=target_shard,
                max_paragraphs=target_capacity,
            )

            # Update shard paragraph counts
            shard_to_merge.paragraphs -= moved_paragraphs
            target_shard.paragraphs += moved_paragraphs
            shards.sort(key=lambda x: x.paragraphs)

            await self.wait_for_indexing()

        if empty_shard:
            # Build the index again, and make sure there is no resource assigned to this shard
            await self.build_shard_resources_index()
            shard_resources = self.index.get(shard_to_merge.id, set())
            if len(shard_resources) > 0:
                logger.error(
                    f"Shard expected to be empty, but it isn't. Won't be deleted.",
                    extra={
                        "kbid": self.kbid,
                        "shard": shard_to_merge.id,
                        "resources": list(shard_resources)[:30],
                    },
                )
                return

            # If shard was emptied, delete it
            async with locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=self.kbid)):
                async with datamanagers.with_rw_transaction() as txn:
                    kb_shards = await datamanagers.cluster.get_kb_shards(
                        txn, kbid=self.kbid, for_update=True
                    )
                    if kb_shards is not None:
                        logger.info(
                            "Deleting empty shard",
                            extra={
                                "kbid": self.kbid,
                                "shard_id": shard_to_merge.id,
                                "nidx_shard_id": shard_to_merge.nidx_id,
                            },
                        )

                        # Delete shards from kb shards in maindb
                        to_delete, to_delete_idx = next(
                            (s, idx)
                            for idx, s in enumerate(kb_shards.shards)
                            if s.shard == shard_to_merge.id
                        )
                        kb_shards.shards.remove(to_delete)
                        if to_delete_idx <= kb_shards.actual:
                            # Only decrement the actual pointer if we remove before the pointer.
                            kb_shards.actual -= 1
                        assert kb_shards.actual >= 0
                        await datamanagers.cluster.update_kb_shards(
                            txn, kbid=self.kbid, shards=kb_shards
                        )
                        await txn.commit()

                # Delete shard from nidx
                await get_nidx_api_client().DeleteShard(
                    noderesources_pb2.ShardId(id=to_delete.nidx_shard_id)
                )


async def get_resource_paragraphs_count(resource_id: str, nidx_shard_id: str) -> int:
    # Do a search on the fields (paragraph) index and return the number of paragraphs this resource has
    try:
        request = nodereader_pb2.SearchRequest(
            shard=nidx_shard_id,
            paragraph=True,
            document=False,
            result_per_page=0,
            field_filter=nodereader_pb2.FilterExpression(
                resource=nodereader_pb2.FilterExpression.ResourceFilter(resource_id=resource_id)
            ),
        )
        search_response: nodereader_pb2.SearchResponse = await get_nidx_searcher_client().Search(request)
        return search_response.paragraph.total
    except AioRpcError as exc:  # pragma: no cover
        if exc.code() == StatusCode.NOT_FOUND:
            logger.warning(f"Shard not found in nidx", extra={"nidx_shard_id": nidx_shard_id})
            return 0
        raise


def get_target_shard(
    shards: list[RebalanceShard], rebalanced_shard: RebalanceShard, skip_active: bool = True
) -> tuple[Optional[RebalanceShard], int]:
    """
    Return the biggest shard with capacity (< 90% of the max paragraphs per shard).
    """
    target_shard = next(
        reversed(
            [
                s
                for s in shards
                if s.id != rebalanced_shard.id
                and s.paragraphs < settings.max_shard_paragraphs * 0.9
                and (not skip_active or (skip_active and not s.active))
            ]
        ),
        None,
    )
    if target_shard is None:  # pragma: no cover
        return None, 0

    # Aim to fill target shards up to 100% of max
    capacity = int(max(0, settings.max_shard_paragraphs - target_shard.paragraphs))
    return target_shard, capacity


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
    except AioRpcError as exc:  # pragma: no cover
        if exc.code() == StatusCode.NOT_FOUND:
            logger.warning(f"Shard not found in nidx", extra={"nidx_shard_id": nidx_shard_id})
            return 0
        raise


async def move_resource_to_shard(
    context: ApplicationContext,
    kbid: str,
    resource_id: str,
    from_shard: writer_pb2.ShardObject,
    to_shard: writer_pb2.ShardObject,
) -> bool:
    indexed_to_new = False
    deleted_from_old = False
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
            if found_shard_id is None:  # pragma: no cover
                # resource deleted
                return False
            if found_shard_id != from_shard.shard:  # pragma: no cover
                # resource could have already been moved
                return False

            await datamanagers.resources.set_resource_shard_id(
                txn, kbid=kbid, rid=resource_id, shard=to_shard.shard
            )
            await index_resource_to_shard(context, kbid, resource_id, to_shard)
            indexed_to_new = True
            await delete_resource_from_shard(context, kbid, resource_id, from_shard)
            deleted_from_old = True
            await txn.commit()
            return True
    except Exception:
        logger.exception(
            "Failed to move resource",
            extra={"kbid": kbid, "resource_id": resource_id},
        )
        # XXX Not ideal failure situation here. Try reverting the whole move even though it could be redundant
        try:
            if indexed_to_new:
                await delete_resource_from_shard(context, kbid, resource_id, to_shard)
            if deleted_from_old:
                await index_resource_to_shard(context, kbid, resource_id, from_shard)
        except Exception:
            logger.exception(
                "Failed to revert move resource. Hopefully you never see this message.",
                extra={"kbid": kbid, "resource_id": resource_id},
            )
        return False


def needs_split(shard: RebalanceShard) -> bool:
    """
    Return true if the shard is more than 110% of the max.

    Active shards are not considered for splitting: the shard creator subscriber will
    eventually create a new shard, make it the active one and the previous one, if
    too full, will be split.
    """
    return not shard.active and (shard.paragraphs > (settings.max_shard_paragraphs * 1.1))


def needs_merge(shard: RebalanceShard, all_shards: list[RebalanceShard]) -> bool:
    """
    Returns true if a shard is less 75% full and there is enough capacity on the other shards to fit it.

    Active shards are not considered for merging. Shards that are more than 75% full are also skipped.
    """
    if shard.active:
        return False
    if shard.paragraphs > (settings.max_shard_paragraphs * 0.75):
        return False
    other_shards = [s for s in all_shards if s.id != shard.id and not s.active]
    other_shards_capacity = sum(
        [max(0, (settings.max_shard_paragraphs - s.paragraphs)) for s in other_shards]
    )
    return shard.paragraphs < other_shards_capacity


async def rebalance_kb(context: ApplicationContext, kbid: str) -> None:
    rebalancer = Rebalancer(context, kbid)
    await rebalancer.rebalance_shards()


async def run(context: ApplicationContext) -> None:
    try:
        async with locking.distributed_lock(REBALANCE_LOCK):
            # get all kb ids
            async with datamanagers.with_ro_transaction() as txn:
                kbids = [kbid async for kbid, _ in datamanagers.kb.get_kbs(txn)]
            # go through each kb and see if shards need to be rebalanced
            for kbid in kbids:
                if not has_feature(
                    const.Features.REBALANCE_ENABLED, default=False, context={"kbid": kbid}
                ):
                    continue
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
