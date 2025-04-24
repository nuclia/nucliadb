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

"""Migration #17

Change how we track active (writable) shrads for a KB. Right now we use the
`writer_pb2.Shards.actual` to point the position of the current shard in the
list of shards.

The migration changes this to a flag in each `writer_pb2.ShardObject`, so in the
future multiple writable shards will be possible.

"""

import logging

from nucliadb.migrator.context import ExecutionContext

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    pass

    # No longer relevant with nidx

    # async with context.kv_driver.transaction() as txn:
    #     shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
    #     if shards is None:
    #         logger.error("KB without shards", extra={"kbid": kbid})
    #         return

    #     for shard_object in shards.shards:
    #         shard_object.read_only = True
    #     shards.shards[shards.actual].read_only = False

    #     # just ensure we're writing it correctly
    #     assert [shard_object.read_only for shard_object in shards.shards].count(False) == 1

    #     await datamanagers.cluster.update_kb_shards(txn, kbid=kbid, shards=shards)
    #     await txn.commit()
