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
    active: bool
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
    kb_shards = await get_kb_shards(kbid)
    if kb_shards is None:
        return []
    return [
        RebalanceShard(
            id=shard.shard,
            nidx_id=shard.nidx_shard_id,
            active=idx == kb_shards.actual,
            paragraphs=await get_shard_paragraph_count(shard.nidx_shard_id),
        )
        for idx, shard in enumerate(kb_shards.shards)
    ]


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


async def rebalance_kb_spagetti(context: ApplicationContext, kbid: str) -> None:
    all_shards = await get_rebalance_shards(kbid)

    # Add a 20% margin to the configured max
    max_shard_paragraphs = settings.max_shard_paragraphs * 1.2

    # For simplicity, skip the active shards from the rebalancing
    non_active_shards = [s for s in all_shards if not s.active]

    # First off, check if any of the shards went over the max paragraphs limit
    over_max = [s for s in non_active_shards if s.paragraphs > max_shard_paragraphs]
    if over_max:
        logger.info(
            "Some shards exceed the paragraphs limit",
            extra={"kbid": kbid, "all_shards": [s.to_dict() for s in all_shards]},
        )
        # Calculate the excedent of paragraphs
        excess = sum(s.paragraphs - max_shard_paragraphs for s in over_max)

        # Other shards that are not over the max limit
        with_room = [s for s in non_active_shards if s not in over_max]

        # Get remaining space in other shards
        room = sum(max_shard_paragraphs - s.paragraphs for s in with_room)

        # Calculate how many new shards are needed to rebalance the excedent paragraphs
        if room > excess:
            # No need to create new shards, as we can fit the excess of paragraphs in the existing shards
            shards_needed = 0
        else:
            # We need to add some shards to make room for the excess.
            shards_needed = math.ceil((excess - room) / max_shard_paragraphs)

        if shards_needed > 0:
            logger.info(
                "We need to add more shards",
                extra={
                    "kbid": kbid,
                    "shards_needed": shards_needed,
                    "all_shards": [s.to_dict() for s in all_shards],
                },
            )
            # Add new shards where to rebalance the excess of paragraphs
            async with (
                locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)),
                datamanagers.with_rw_transaction() as txn,
            ):
                sm = get_shard_manager()
                for _ in range(shards_needed):
                    await sm.create_shard_by_kbid(txn, kbid)
                await txn.commit()

            # Recalculate after having created shards, the active shard is a different one
            all_shards = await get_rebalance_shards(kbid)
            non_active_shards = [s for s in all_shards if not s.active]
            over_max = [s for s in non_active_shards if s.paragraphs > max_shard_paragraphs]
            with_room = [s for s in non_active_shards if s not in over_max]

        # Now move the excess of paragraphs to other shards where there is space
        for over_max_shard in over_max:
            logger.info(
                "Moving excess of paragraphs to other shards",
                extra={
                    "kbid": kbid,
                    "source": over_max_shard.to_dict(),
                    "all_shards": [s.to_dict() for s in all_shards],
                },
            )
            # Keep moving resoures to other shards as long as we are still over the max
            shard_paragraphs = over_max_shard.paragraphs
            while shard_paragraphs > max_shard_paragraphs:
                # Get the biggest shard with some space left
                try:
                    target_shard = list(sorted(with_room, key=lambda x: x.paragraphs))[-1]
                except IndexError:
                    # No target shard found
                    break

                # Move some resources to the new shard
                await move_set_of_kb_resources(
                    context, kbid, from_shard_id=over_max_shard.id, to_shard_id=target_shard.id, count=20
                )

                # Recalculate counts
                all_shards = await get_rebalance_shards(kbid)
                shard_paragraphs = next(s.paragraphs for s in all_shards if s.id == over_max_shard.id)

                non_active_shards = [s for s in all_shards if not s.active]
                with_room = [
                    s
                    for s in non_active_shards
                    if s.paragraphs < max_shard_paragraphs and s.id != over_max_shard.id
                ]

    # Now check if some shards could be merged into smaller ones
    shards_to_delete: list[RebalanceShard] = []
    can_merge_shards = len(non_active_shards) > (
        sum(s.paragraphs for s in non_active_shards) / max_shard_paragraphs
    )
    while can_merge_shards:
        # Take the smallest shard and move some resources to other shards until it gets empty
        non_active_shards.sort(key=lambda x: x.paragraphs)
        smallest_shard = non_active_shards[0]
        logger.info(
            "Merging smallest shard",
            extra={
                "kbid": kbid,
                "smallest": smallest_shard.id,
                "all_shards": [s.to_dict() for s in all_shards],
            },
        )
        while True:
            resources_count = await get_shard_resources_count(smallest_shard.nidx_id)
            if resources_count == 0:
                logger.info(
                    "Shard is now empty",
                    extra={
                        "kbid": kbid,
                        "smallest": smallest_shard.id,
                        "all_shards": [s.to_dict() for s in all_shards],
                    },
                )
                shards_to_delete.append(smallest_shard)
                break

            logger.info(
                "Shard not yet empty",
                extra={
                    "kbid": kbid,
                    "smallest": smallest_shard.id,
                    "remaining": resources_count,
                },
            )

            # Take the biggest shard that has some room (90% of the max, to prevent filling them completely)
            with_room = [
                s
                for s in non_active_shards
                if s.paragraphs < settings.max_shard_paragraphs * 0.9 and s.id != smallest_shard.id
            ]
            with_room.sort(key=lambda x: x.paragraphs)
            try:
                target_shard = with_room[-1]
            except IndexError:
                # No target shard could be found. Move on
                break

            await move_set_of_kb_resources(
                context, kbid, from_shard_id=smallest_shard.id, to_shard_id=target_shard.id, count=20
            )

            # Recompute counts
            all_shards = await get_rebalance_shards(kbid)
            non_active_shards = [s for s in all_shards if not s.active]

        # Recompute counts
        all_shards = await get_rebalance_shards(kbid)
        non_active_shards = [s for s in all_shards if not s.active]
        can_merge_shards = len(non_active_shards) > (
            sum(s.paragraphs for s in non_active_shards) / max_shard_paragraphs
        )

    if shards_to_delete:
        async with locking.distributed_lock(locking.NEW_SHARD_LOCK.format(kbid=kbid)):
            kb_shards = await get_kb_shards(kbid)
            if kb_shards is not None:
                # Delete shards from nidx
                for shard in shards_to_delete:
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
