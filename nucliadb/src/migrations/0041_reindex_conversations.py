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
import logging
import uuid
from collections.abc import AsyncIterator
from typing import cast

from nucliadb.common import datamanagers
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.ingest.orm.index_message import get_resource_index_message
from nucliadb.ingest.orm.resource import Resource
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.writer_pb2 import ShardObject, Shards

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    """
    Reindex resources that have conversation fields
    """
    kb_shards = await datamanagers.atomic.cluster.get_kb_shards(kbid=kbid, for_update=False)
    if kb_shards is not None:
        async for rid in iter_affected_resource_ids(context, kbid):
            await reindex_resource(context, kbid, rid, kb_shards)
    else:
        logger.warning(
            "Migration 41: KB shards not found, skipping reindexing",
            extra={"kbid": kbid},
        )


async def reindex_resource(
    context: ExecutionContext,
    kbid: str,
    rid: str,
    kb_shards: Shards,
) -> None:
    """
    Reindex a single resource
    """
    async with datamanagers.with_ro_transaction() as rs_txn:
        # Fetch the resource
        resource = await Resource.get(rs_txn, kbid=kbid, rid=rid)
        if resource is None:
            logger.warning(
                "Migration 41: Resource not found, skipping reindexing",
                extra={"kbid": kbid, "rid": rid},
            )
            return

        # Get the shard for the resource
        shard: ShardObject | None = None
        shard_id = await datamanagers.resources.get_resource_shard_id(
            rs_txn, kbid=kbid, rid=rid, for_update=False
        )
        if shard_id is not None:
            shard = next((shard for shard in kb_shards.shards if shard.shard == shard_id), None)
        if shard is None:
            logger.warning(
                "Migration 41: Shard not found for resource, skipping reindexing",
                extra={"kbid": kbid, "rid": rid, "shard_id": shard_id},
            )
            return

        # Create the index message and reindex the resource
        index_message = await get_resource_index_message(resource, reindex=True)
        await context.shard_manager.add_resource(
            shard,
            index_message,
            0,
            partition="0",
            kb=kbid,
            reindex_id=uuid.uuid4().hex,
        )
        logger.info(
            "Migration 41: Resource reindexed",
            extra={"kbid": kbid, "rid": rid},
        )


async def iter_affected_resource_ids(context: ExecutionContext, kbid: str) -> AsyncIterator[str]:
    start = ""
    while True:
        keys_batch = await get_batch(context, kbid, start)
        if keys_batch is None:
            break
        start = keys_batch[-1]
        for key in keys_batch:
            # The keys have the format /kbs/{kbid}/r/{rid}/f/c/{field_id}
            rid = key.split("/")[4]
            yield rid


async def get_batch(context: ExecutionContext, kbid: str, start: str) -> list[str] | None:
    """
    Get a batch of resource keys that hold conversation fields for the given KB.
    Starting after the given start key.
    Returns None if no more keys are found.
    """
    batch_size = 100
    async with context.kv_driver.rw_transaction() as txn:
        txn = cast(PGTransaction, txn)
        async with txn.connection.cursor() as cur:
            await cur.execute(
                """
                SELECT key FROM resources
                WHERE key ~ ('^/kbs/' || %s || '/r/[^/]*/f/c/[^/]*$')
                AND key > %s
                ORDER BY key
                LIMIT %s""",
                (kbid, start, batch_size),
            )
            rows = await cur.fetchall()
            if len(rows) == 0:
                return None
            return [row[0] for row in rows]
