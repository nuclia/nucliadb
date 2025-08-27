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

import argparse
import asyncio
import importlib.metadata
from typing import Optional

from grpc.aio import AioRpcError
from nidx_protos import nodereader_pb2, noderesources_pb2

from nucliadb.common import datamanagers
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.common.nidx import (
    get_nidx_api_client,
    start_nidx_utility,
    stop_nidx_utility,
)
from nucliadb.ingest import logger
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging

ShardKb = str


UNKNOWN_KB = "unknown"


async def detect_orphan_shards(driver: Driver) -> dict[str, ShardKb]:
    """Detect orphan shards in the system. An orphan shard is one indexed but
    not referenced for any stored KB.

    """
    # To avoid detecting a new shard as orphan, query the index first and maindb
    # afterwards
    indexed_shards = await _get_indexed_shards()
    stored_shards = await _get_stored_shards(driver)

    # In normal conditions, this should never happen as shards are created first
    # in the index and then in maindb. However, if a new shard has been created
    # between index and maindb scans, we can also see it here
    not_indexed_shards = stored_shards.keys() - indexed_shards.keys()
    for shard_id in not_indexed_shards:
        kbid = stored_shards[shard_id]
        logger.info(
            "Found a shard on maindb not indexed in the index nodes. "
            "This can be either a shard not indexed (error) or a brand new shard. "
            "If you run purge and find it again, it's probably an error",
            extra={
                "shard_id": shard_id,
                "kbid": kbid,
            },
        )

    orphan_shard_ids = indexed_shards.keys() - stored_shards.keys()
    orphan_shards: dict[str, ShardKb] = {}
    for shard_id in orphan_shard_ids:
        kbid = await _get_kbid(shard_id) or UNKNOWN_KB
        # Shards with knwon KB ids can be checked and ignore those comming from
        # an ongoing migration/rollover (ongoing or finished)
        if kbid != UNKNOWN_KB:
            async with datamanagers.with_ro_transaction() as txn:
                skip = await datamanagers.rollover.is_rollover_shard(
                    txn, kbid=kbid, shard_id=shard_id
                ) or await datamanagers.cluster.is_kb_shard(txn, kbid=kbid, shard_id=shard_id)
                if skip:
                    continue
        orphan_shards[shard_id] = kbid

    for shard_id in orphan_shard_ids:
        kbid = await _get_kbid(shard_id) or UNKNOWN_KB
        orphan_shards[shard_id] = kbid
    return orphan_shards


async def _get_indexed_shards() -> dict[str, ShardKb]:
    shards = await get_nidx_api_client().ListShards(noderesources_pb2.EmptyQuery())

    return {shard.id: UNKNOWN_KB for shard in shards.ids}


async def _get_stored_shards(driver: Driver) -> dict[str, ShardKb]:
    stored_shards: dict[str, ShardKb] = {}

    async with driver.ro_transaction() as txn:
        async for kbid, _ in datamanagers.kb.get_kbs(txn):
            kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
            if kb_shards is None:
                logger.warning("KB not found while looking for orphan shards", extra={"kbid": kbid})
                continue

            for shard_object_pb in kb_shards.shards:
                stored_shards[shard_object_pb.nidx_shard_id] = kbid

    return stored_shards


async def _get_kbid(shard_id: str) -> Optional[str]:
    kbid = None
    try:
        req = nodereader_pb2.GetShardRequest()
        req.shard_id.id = shard_id
        shard_pb = await get_nidx_api_client().GetShard(req)
    except AioRpcError as grpc_error:
        logger.error(
            "Can't get shard while looking for orphans in nidx, is there something broken?",
            exc_info=grpc_error,
            extra={
                "shard_id": shard_id,
            },
        )
    else:
        kbid = shard_pb.metadata.kbid
    return kbid


async def report_orphan_shards(driver: Driver):
    orphan_shards = await detect_orphan_shards(driver)
    logger.info(f"Found {len(orphan_shards)} orphan shards")
    async with driver.ro_transaction() as txn:
        for shard_id, kbid in orphan_shards.items():
            if kbid == UNKNOWN_KB:
                msg = "Found orphan shard but could not get KB info"
            else:
                kb_exists = await datamanagers.kb.exists_kb(txn, kbid=kbid)
                if kb_exists:
                    msg = "Found orphan shard for existing KB"
                else:
                    msg = "Found orphan shard for already removed KB"

            logger.debug(
                msg,
                extra={
                    "shard_id": shard_id,
                    "kbid": kbid,
                },
            )


async def purge_orphan_shards(driver: Driver):
    orphan_shards = await detect_orphan_shards(driver)
    logger.info(f"Found {len(orphan_shards)} orphan shards. Purge starts...")

    for shard_id, kbid in orphan_shards.items():
        logger.info(
            "Deleting orphan shard from index node",
            extra={
                "shard_id": shard_id,
                "kbid": kbid,
            },
        )
        req = noderesources_pb2.ShardId(id=shard_id)
        await get_nidx_api_client().DeleteShard(req)


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--purge",
        action="store_true",
        default=False,
        required=False,
        help="Purge detected orphan shards",
    )
    args = parser.parse_args()
    return args


async def main():
    """This script will detect orphan shards, i.e., indexed shards with no
    reference in our source of truth.

    Purging a knowledgebox can lead to orphan shards if index nodes are not
    available or fail. It's possible that other procedures in our database left
    orphan shards too.

    ATENTION! In the future, some new process which adds new shards could be
    implemented. If orphan shard detection is not updated, this will lead to
    incorrect detection. To avoid data loss/corruption, it is highly recommended
    to don't remove orphan shards that hasn't exist for a long time.

    """
    args = parse_arguments()

    await start_nidx_utility()
    await setup_cluster()
    driver = await setup_driver()

    try:
        if args.purge:
            await purge_orphan_shards(driver)
        else:
            await report_orphan_shards(driver)

    finally:
        await teardown_driver()
        await teardown_cluster()
        await stop_nidx_utility()


def run() -> int:  # pragma: no cover
    setup_logging()

    errors.setup_error_handling(importlib.metadata.distribution("nucliadb").version)

    return asyncio.run(main())
