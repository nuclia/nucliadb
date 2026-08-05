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
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import writer_pb2

from .utils import _pg_cursor


async def get_kb_shards(
    txn: Transaction, *, kbid: str, for_update: bool = False
) -> writer_pb2.Shards | None:
    async with _pg_cursor(txn) as cur:
        statement = "SELECT shards FROM kbs WHERE kbid = %(kbid)s"
        if for_update:
            statement += " FOR UPDATE"
        await cur.execute(statement, {"kbid": kbid})
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = writer_pb2.Shards()
        pb.ParseFromString(row[0])
        return pb


async def is_kb_shard(txn: Transaction, *, kbid: str, shard_id: str) -> bool:
    shards = await get_kb_shards(txn, kbid=kbid)
    if shards is None:
        return False
    for shard in shards.shards:
        if shard.shard == shard_id:
            return True
    return False


async def update_kb_shards(txn: Transaction, *, kbid: str, shards: writer_pb2.Shards) -> None:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kbs (kbid, shards)
            VALUES (%(kbid)s, %(shards)s)
            ON CONFLICT (kbid) DO UPDATE SET
                shards = EXCLUDED.shards
            """,
            {"kbid": kbid, "shards": shards.SerializeToString()},
        )
