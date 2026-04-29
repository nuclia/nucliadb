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
from typing import cast, overload

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGDriver, PGTransaction
from nucliadb.common.maindb.utils import get_driver
from nucliadb_telemetry import metrics

logger = logging.getLogger(__name__)

observer = metrics.Observer("nucliadb_file_md5", labels={"op": ""})


def _pg_driver() -> PGDriver:
    return cast(PGDriver, get_driver())


def _pg_transaction(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


@observer.wrap({"op": "check"})
async def exists(*, kbid: str, md5: str) -> bool:
    """Check if a file with the given MD5 hash already exists in the KB."""
    pg = _pg_driver()
    async with pg._get_connection() as conn, conn.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM file_md5 WHERE kbid = %(kbid)s AND md5 = %(md5)s LIMIT 1",
            {"kbid": kbid, "md5": md5},
        )
        return cur.rowcount > 0


@observer.wrap({"op": "store"})
async def set(txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str) -> None:
    """Set a file MD5 hash for a resource field."""
    async with _pg_transaction(txn).connection.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO file_md5 (kbid, md5, rid, field_id)
            VALUES (%(kbid)s, %(md5)s, %(rid)s, %(field_id)s)
            ON CONFLICT (kbid, md5, rid, field_id) DO UPDATE SET
                created_at = NOW()
            """,
            {"kbid": kbid, "md5": md5, "rid": rid, "field_id": field_id},
        )


@overload
async def delete(txn: Transaction, *, kbid: str) -> None: ...


@overload
async def delete(txn: Transaction, *, kbid: str, rid: str) -> None: ...


@overload
async def delete(txn: Transaction, *, kbid: str, rid: str, field_id: str) -> None: ...


@observer.wrap({"op": "delete"})
async def delete(
    txn: Transaction,
    *,
    kbid: str,
    rid: str | None = None,
    field_id: str | None = None,
) -> None:
    """Delete file MD5 records.

    - kbid only: delete all records for the KB
    - kbid + rid: delete all records for a resource
    - kbid + rid + field_id: delete records for a specific field
    """
    pg_txn = _pg_transaction(txn)
    if rid is None:
        async with pg_txn.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM file_md5 WHERE kbid = %(kbid)s",
                {"kbid": kbid},
            )
    elif field_id is None:
        async with pg_txn.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM file_md5 WHERE kbid = %(kbid)s AND rid = %(rid)s",
                {"kbid": kbid, "rid": rid},
            )
    else:
        async with pg_txn.connection.cursor() as cur:
            await cur.execute(
                "DELETE FROM file_md5 WHERE kbid = %(kbid)s AND rid = %(rid)s AND field_id = %(field_id)s",
                {"kbid": kbid, "rid": rid, "field_id": field_id},
            )
