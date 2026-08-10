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

"""Migration #51

Fix conversation fields where page 1 is missing due to a replace_field bug.

When set_value was called with replace_field=True, delete_value() removed all
rows from kb_conversations but left the stale FieldConversation metadata in
kb_fields.  The next set_value then re-fetched that stale metadata (pages=N),
hit PageNotFound for the last page, and incremented pages to N+1, storing new
messages starting at page N+1 instead of page 1.

This migration finds every conversation field whose earliest real page number
is greater than 1 and shifts all pages down so page 1 is always the first
page.  It also corrects the FieldConversation.pages count in kb_fields and
updates the per-message page references in SplitsMetadata.
"""

import logging
from typing import cast

from nucliadb.common.datamanagers import conversations
from nucliadb.common.datamanagers.utils import _pg_cursor
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos.resources_pb2 import FieldConversation

logger = logging.getLogger(__name__)


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:
    broken: list[tuple[str, str, int]] = []
    async with context.kv_driver.rw_transaction() as txn:
        pg_txn = cast(PGTransaction, txn)
        async with pg_txn.connection.cursor() as cur:
            await cur.execute(
                """
                SELECT rid, field_id, MIN(page) AS min_page
                FROM kb_conversations
                WHERE kbid = %s AND page > 0
                GROUP BY rid, field_id
                HAVING MIN(page) > 1
                """,
                (kbid,),
            )
            rows = await cur.fetchall()
        # read-only scan; no need to commit
        await txn.abort()

    for rid, field_id, min_page in rows:
        broken.append((rid, field_id, min_page))

    if not broken:
        return

    logger.info(
        "Found conversation fields with missing page 1",
        extra={"kbid": kbid, "count": len(broken)},
    )

    for rid, field_id, min_page in broken:
        async with context.kv_driver.rw_transaction() as txn:
            await _fix_field(txn, kbid=kbid, rid=rid, field_id=field_id, min_page=min_page)
            await txn.commit()
            logger.info(
                "Fixed conversation field page numbering",
                extra={"kbid": kbid, "rid": rid, "field_id": field_id, "offset": min_page - 1},
            )


async def _fix_field(txn: Transaction, *, kbid: str, rid: str, field_id: str, min_page: int) -> None:
    offset = min_page - 1

    async with _pg_cursor(txn) as cur:
        # Read all real pages ordered so we can reinsert them
        await cur.execute(
            """
            SELECT page, value FROM kb_conversations
            WHERE kbid = %s AND rid = %s AND field_type = 'c' AND field_id = %s AND page > 0
            ORDER BY page
            """,
            (kbid, rid, field_id),
        )
        pages = await cur.fetchall()  # [(old_page, raw_bytes), ...]

        # Delete then reinsert to avoid PK conflicts during renumbering
        await cur.execute(
            """
            DELETE FROM kb_conversations
            WHERE kbid = %s AND rid = %s AND field_type = 'c' AND field_id = %s AND page > 0
            """,
            (kbid, rid, field_id),
        )
        for old_page, value in pages:
            await cur.execute(
                """
                INSERT INTO kb_conversations (kbid, rid, field_type, field_id, page, value)
                VALUES (%s, %s, 'c', %s, %s, %s)
                """,
                (kbid, rid, field_id, old_page - offset, value),
            )

        # Correct the FieldConversation.pages count stored in kb_fields
        await cur.execute(
            """
            SELECT value FROM kb_fields
            WHERE kbid = %s AND rid = %s AND field_type = 'c' AND field_id = %s
            """,
            (kbid, rid, field_id),
        )
        row = await cur.fetchone()
        if row and row[0]:
            fc = FieldConversation()
            fc.ParseFromString(bytes(row[0]))
            fc.pages -= offset
            await cur.execute(
                """
                UPDATE kb_fields SET value = %s
                WHERE kbid = %s AND rid = %s AND field_type = 'c' AND field_id = %s
                """,
                (fc.SerializeToString(), kbid, rid, field_id),
            )

    # Shift page references in SplitsMetadata
    splits_metadata = await conversations.get_splits_metadata(txn, kbid=kbid, rid=rid, field_id=field_id)
    if splits_metadata is not None:
        for ident in splits_metadata.metadata:
            entry = splits_metadata.metadata[ident]
            if entry.page > 0:
                entry.page -= offset
        await conversations.set_splits_metadata(
            txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits_metadata
        )
