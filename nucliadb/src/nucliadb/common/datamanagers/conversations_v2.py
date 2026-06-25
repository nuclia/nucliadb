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
Datamanager for the `kb_conversations` PostgreSQL table (migration 0016).

Each row stores one page of a conversation field, or the SplitsMetadata sentinel:
  - kbid     - FK → kb_resources.kbid (ON DELETE CASCADE)
  - rid      - FK → kb_resources.rid  (ON DELETE CASCADE)
  - field_id - user-defined conversation field name
  - page     - 1-based page number; the sentinel value 0 stores SplitsMetadata
  - value    - serialised protobuf bytes:
                 page = 0  → resources_pb2.SplitsMetadata
                 page >= 1 → resources_pb2.Conversation (~200 messages each)

The FieldConversation metadata (page count, total, page size, extract/split strategy)
is stored in kb_fields.value for the corresponding 'c'-type field row and is written
directly here to avoid a cross-module import cycle with fields_v2.

NOTE: deleting a kb_resources row (or its parent kbs row) automatically removes all
related kb_conversations rows via the ON DELETE CASCADE foreign key.
Deleting a single conversation field requires an explicit DELETE by (kbid, rid, field_id)
because there is no FK from kb_conversations to kb_fields.
"""

from typing import cast

from nucliadb.common.datamanagers.utils import _pg_cursor
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata

# Sentinel page number used to store SplitsMetadata in the kb_conversations table.
# Real conversation pages are 1-based, so 0 is always free.
_SPLITS_METADATA_PAGE = 0


def _pg(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


# ---------------------------------------------------------------------------
# FieldConversation metadata  (stored in kb_fields, field_type = 'c')
# ---------------------------------------------------------------------------


async def get_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> FieldConversation | None:
    """Return the FieldConversation metadata for a conversation field, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT value FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = 'c' AND field_id = %(field_id)s
            """,
            {"kbid": kbid, "rid": rid, "field_id": field_id},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = FieldConversation()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def set_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    metadata: FieldConversation,
) -> None:
    """Upsert the FieldConversation metadata row in kb_fields."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_fields (kbid, rid, field_type, field_id, value)
            VALUES (%(kbid)s, %(rid)s, 'c', %(field_id)s, %(value)s)
            ON CONFLICT (kbid, rid, field_type, field_id) DO UPDATE SET
                value = EXCLUDED.value
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_id": field_id,
                "value": metadata.SerializeToString(),
            },
        )


# ---------------------------------------------------------------------------
# Conversation pages  (stored in kb_conversations, page >= 1)
# ---------------------------------------------------------------------------


async def get_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    page: int,
) -> PBConversation | None:
    """Return one page of a conversation field, or None if not found."""
    if page <= 0:
        raise ValueError("Conversation pages start at index 1")
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT value FROM kb_conversations
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_id = %(field_id)s AND page = %(page)s
            """,
            {"kbid": kbid, "rid": rid, "field_id": field_id, "page": page},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = PBConversation()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def set_page(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    page: int,
    value: PBConversation,
) -> None:
    """Upsert one page of a conversation field."""
    if page <= 0:
        raise ValueError("Conversation pages start at index 1")
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_conversations (kbid, rid, field_id, page, value)
            VALUES (%(kbid)s, %(rid)s, %(field_id)s, %(page)s, %(value)s)
            ON CONFLICT (kbid, rid, field_id, page) DO UPDATE SET
                value = EXCLUDED.value
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_id": field_id,
                "page": page,
                "value": value.SerializeToString(),
            },
        )


# ---------------------------------------------------------------------------
# SplitsMetadata  (stored in kb_conversations at sentinel page = 0)
# ---------------------------------------------------------------------------


async def get_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> SplitsMetadata | None:
    """Return the SplitsMetadata for a conversation field, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT value FROM kb_conversations
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_id = %(field_id)s AND page = %(page)s
            """,
            {"kbid": kbid, "rid": rid, "field_id": field_id, "page": _SPLITS_METADATA_PAGE},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = SplitsMetadata()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def set_splits_metadata(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
    splits_metadata: SplitsMetadata,
) -> None:
    """Upsert the SplitsMetadata sentinel row (page = 0) for a conversation field."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_conversations (kbid, rid, field_id, page, value)
            VALUES (%(kbid)s, %(rid)s, %(field_id)s, %(page)s, %(value)s)
            ON CONFLICT (kbid, rid, field_id, page) DO UPDATE SET
                value = EXCLUDED.value
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_id": field_id,
                "page": _SPLITS_METADATA_PAGE,
                "value": splits_metadata.SerializeToString(),
            },
        )


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def delete_field(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: str,
) -> None:
    """
    Delete all kb_conversations rows for this field (all pages + the
    SplitsMetadata sentinel).  The kb_fields row for the 'c'-type field
    is handled by the fields datamanager; cascading from kb_resources only
    fires when the whole resource or KB is deleted.
    """
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            DELETE FROM kb_conversations
            WHERE kbid = %(kbid)s AND rid = %(rid)s AND field_id = %(field_id)s
            """,
            {"kbid": kbid, "rid": rid, "field_id": field_id},
        )
