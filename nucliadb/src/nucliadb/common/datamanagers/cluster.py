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
from typing import Optional

from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2

from .utils import get_kv_pb

logger = logging.getLogger(__name__)


KB_SHARDS = "/kbs/{kbid}/shards"


async def get_kb_shards(
    txn: Transaction, *, kbid: str, for_update: bool = False
) -> Optional[writer_pb2.Shards]:
    key = KB_SHARDS.format(kbid=kbid)
    return await get_kv_pb(txn, key, writer_pb2.Shards, for_update=for_update)


async def is_kb_shard(txn: Transaction, *, kbid: str, shard_id: str, for_update: bool = False) -> bool:
    shards = await get_kb_shards(txn, kbid=kbid, for_update=for_update)
    if shards is None:
        return False
    for shard in shards.shards:
        if shard.shard == shard_id:
            return True
    return False


async def update_kb_shards(txn: Transaction, *, kbid: str, shards: writer_pb2.Shards) -> None:
    key = KB_SHARDS.format(kbid=kbid)
    await txn.set(key, shards.SerializeToString())
