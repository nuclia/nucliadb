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
"""
V2 implementation of file MD5 tracking using the kb_fields.md5 column
(migration 0016).

Instead of the dedicated `file_md5` table, the MD5 hash is stored directly
in the `kb_fields` row for the corresponding file field (field_type = 'f').
This avoids a separate table and keeps the hash co-located with the field data,
relying on the existing index on (kbid, md5) in kb_fields for efficient lookups.
"""

from typing import cast

from nucliadb.common.datamanagers.utils import _pg_cursor
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver


def _pg_driver() -> PGDriver:
    return cast(PGDriver, get_driver())


async def exists(*, kbid: str, md5: str) -> bool:
    """Check if a file with the given MD5 hash already exists in the KB."""
    pg = _pg_driver()
    async with pg._get_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM kb_fields WHERE kbid = %(kbid)s AND md5 = %(md5)s LIMIT 1",
            {"kbid": kbid, "md5": md5},
        )
        return cur.rowcount > 0


async def set(txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str) -> None:
    """Set the MD5 hash on the kb_fields row for the given file field.

    The field row is expected to already exist (created by fields_v2.set in the
    same transaction). If it does not exist for any reason the UPDATE is a no-op.
    """
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            UPDATE kb_fields SET md5 = %(md5)s
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = 'f' AND field_id = %(field_id)s
            """,
            {"kbid": kbid, "md5": md5, "rid": rid, "field_id": field_id},
        )
