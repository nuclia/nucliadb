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
import contextlib
import logging
from typing import Optional

from nucliadb.common.maindb.driver import Driver, Transaction
from nucliadb_protos import writer_pb2
from nucliadb_utils.keys import KB_SHARDS  # this should be defined here

from .utils import get_kv_pb

logger = logging.getLogger(__name__)


class ClusterDataManager:
    def __init__(self, driver: Driver, *, read_only_txn: Optional[Transaction] = None):
        self.driver = driver
        self._read_only_txn = read_only_txn

    @contextlib.asynccontextmanager
    async def read_only_transaction(self, wait_for_abort: bool = True):
        if self._read_only_txn is not None:
            yield self._read_only_txn
        else:
            async with self.driver.transaction(
                wait_for_abort=wait_for_abort, read_only=True
            ) as txn:
                yield txn

    async def get_kb_shards(self, kbid: str) -> Optional[writer_pb2.Shards]:
        key = KB_SHARDS.format(kbid=kbid)
        async with self.read_only_transaction(wait_for_abort=False) as txn:
            return await get_kv_pb(txn, key, writer_pb2.Shards)

    async def update_kb_shards(self, kbid: str, kb_shards: writer_pb2.Shards) -> None:
        key = KB_SHARDS.format(kbid=kbid)
        async with self.driver.transaction() as txn:
            await txn.set(key, kb_shards.SerializeToString())
            await txn.commit()

    async def get_kb_shard(
        self, kbid: str, shard_id: str
    ) -> Optional[writer_pb2.ShardObject]:
        shards = await self.get_kb_shards(kbid)
        if shards is not None:
            for shard in shards.shards:
                if shard.shard == shard_id:
                    return shard

        return None
