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

"""Migration #19

Targeted rollover for a specific KBs which still don't have the latest version of the paragraphs index
"""

import logging

from nucliadb.common import datamanagers
from nucliadb.common.cluster.rollover import rollover_kb_shards
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.noderesources_pb2 import ShardCreated

logger = logging.getLogger(__name__)


class ShardsObjectNotFound(Exception): ...


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    try:
        if await has_old_paragraphs_index(context, kbid):
            logger.info("Rolling over affected KB", extra={"kbid": kbid})
            await rollover_kb_shards(context, kbid)
        else:
            logger.info(
                "KB already has the latest version of the paragraphs index, skipping rollover",
                extra={"kbid": kbid},
            )
    except ShardsObjectNotFound:
        logger.warning("KB not found, skipping rollover", extra={"kbid": kbid})


async def has_old_paragraphs_index(context: ExecutionContext, kbid: str) -> bool:
    async with context.kv_driver.transaction(read_only=True) as txn:
        shards_object = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if not shards_object:
            raise ShardsObjectNotFound()
        for shard in shards_object.shards:
            for replica in shard.replicas:
                if replica.shard.paragraph_service != ShardCreated.ParagraphService.PARAGRAPH_V3:
                    return True
        return False
