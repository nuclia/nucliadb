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

from nucliadb.common import datamanagers
from nucliadb.ingest.orm.index_message import get_resource_index_message
from nucliadb.ingest.orm.resource import Resource
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.resources_pb2 import FieldType

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    """
    Reindex resources that have conversation fields
    """
    affected_resource_ids = set()
    async with datamanagers.with_ro_transaction() as txn:
        async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
            field_ids = await datamanagers.resources.get_all_field_ids(
                txn, kbid=kbid, rid=rid, for_update=False
            )
            if field_ids is None:
                continue
            for fid in field_ids.fields:
                if fid.field_type == FieldType.CONVERSATION:
                    affected_resource_ids.add(rid)
                    break

    if len(affected_resource_ids) == 0:
        logger.info(
            "Migration 41: No resources with conversation fields to reindex in KB",
            extra={"kbid": kbid},
        )
        return

    kb_shards = await datamanagers.atomic.cluster.get_kb_shards(kbid=kbid, for_update=False)
    if kb_shards is None:
        logger.warning(
            "Migration 41: KB shards not found, skipping reindexing",
            extra={"kbid": kbid},
        )
        return

    logger.info(
        f"Migration 41: Reindexing {len(affected_resource_ids)} resources with conversation fields in KB",
        extra={"kbid": kbid},
    )
    for rid in affected_resource_ids:
        async with datamanagers.with_ro_transaction() as rs_txn:
            resource = await Resource.get(rs_txn, kbid=kbid, rid=rid)
            if resource is None:
                logger.warning(
                    "Migration 41: Resource not found, skipping reindexing",
                    extra={"kbid": kbid, "rid": rid},
                )
                continue
            shard_id = await datamanagers.resources.get_resource_shard_id(
                rs_txn, kbid=kbid, rid=rid, for_update=False
            )
            if shard_id is None:
                logger.warning(
                    "Migration 41: Shard ID not found for resource, skipping reindexing",
                    extra={"kbid": kbid, "rid": rid},
                )
                continue
            shard = next((shard for shard in kb_shards.shards if shard.shard == shard_id), None)
            if shard is None:
                logger.warning(
                    "Migration 41: Shard not found for resource, skipping reindexing",
                    extra={"kbid": kbid, "rid": rid, "shard_id": shard_id},
                )
                continue
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
