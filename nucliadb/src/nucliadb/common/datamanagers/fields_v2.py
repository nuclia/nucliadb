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
Datamanager for the `fields` PostgreSQL table (migration 0016).

Each row represents one field in a resource and stores:
  - kbid       - FK → kb_resources.kbid (ON DELETE CASCADE)
  - rid        - FK → kb_resources.rid  (ON DELETE CASCADE)
  - field_type - single-char abbreviation: t=text, f=file, u=link,
                 c=conversation, a=generic, k=key_value
  - field_id   - user-defined field name
  - status     - serialised writer_pb2.FieldStatus protobuf bytes; NULL when not yet set
  - value      - serialised protobuf bytes (field payload, excluding
                 anything stored in object storage)
  - md5        - optional content hash; NULL when not provided; used for
                 duplicate detection within a knowledge box

NOTE: deleting a kb_resources row (or its parent kbs row) automatically
removes all related field rows via the ON DELETE CASCADE foreign key —
there is no need for explicit bulk-delete helpers here.
"""

from typing import Sequence, cast

from google.protobuf.message import Message

from nucliadb.common.datamanagers.utils import _pg_cursor
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pg(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


async def set_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: wpb2.FieldStatus,
) -> None:
    """Update only the status column. Does nothing if the row does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            UPDATE kb_fields SET status = %(status)s
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = %(field_type)s AND field_id = %(field_id)s
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
                "status": status.SerializeToString(),
            },
        )


async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    value: Message,
) -> None:
    """
    Set the value of a field row, creating it if it does not exist. This is an upsert operation.
    """
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_fields (kbid, rid, field_type, field_id, value)
            VALUES (%(kbid)s, %(rid)s, %(field_type)s, %(field_id)s, %(value)s)
            ON CONFLICT (kbid, rid, field_type, field_id) DO UPDATE SET
                value = EXCLUDED.value
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
                "value": value.SerializeToString(),
            },
        )


async def delete(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> None:
    """Delete a single field row."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            DELETE FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = %(field_type)s AND field_id = %(field_id)s
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
            },
        )


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


async def get_raw(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> bytes | None:
    """Return only the serialised value bytes for a field, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT value FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = %(field_type)s AND field_id = %(field_id)s
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
            },
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        return bytes(row[0])


async def get_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> wpb2.FieldStatus | None:
    """Return the deserialised FieldStatus for a field, or None if the row does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT status FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = %(field_type)s AND field_id = %(field_id)s
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
            },
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = wpb2.FieldStatus()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def get_statuses(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    fields: Sequence[rpb2.FieldID],
) -> list[wpb2.FieldStatus]:
    """Return the deserialised FieldStatus for a list of fields, in the same order."""
    if not fields:
        return []

    async with _pg_cursor(txn) as cur:
        await cur.execute(
            f"""
            SELECT field_type, field_id, status
            FROM kb_fields
            WHERE kbid = %s AND rid = %s
              AND (field_type, field_id) IN (
                {",".join(["(%s, %s)"] * len(fields))}
              )
            """,
            [kbid, rid]
            + [
                item
                for f in fields
                for item in (from_proto.field_type_name(f.field_type).abbreviation(), f.field)
            ],
        )
        rows = await cur.fetchall()

    # Build a lookup dict for fast access
    status_lookup = {(row[0], row[1]): bytes(row[2]) if row[2] is not None else None for row in rows}

    result = []
    for f in fields:
        status_bytes = status_lookup.get((f.field_type, f.field))
        if status_bytes is None:
            result.append(wpb2.FieldStatus())  # Default empty status
        else:
            pb = wpb2.FieldStatus()
            pb.ParseFromString(status_bytes)
            result.append(pb)

    return result


async def get_all_field_ids(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
) -> rpb2.AllFieldIDs:
    """Return the AllFieldIDs protobuf for a resource, or None if not set."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT field_type, field_id FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
            """,
            {"kbid": kbid, "rid": rid},
        )
        pb = rpb2.AllFieldIDs()
        rows = await cur.fetchall()
        for row in rows:
            field = pb.fields.add()
            field.field_type = to_proto.field_type(row[0])
            field.field = row[1]
        return pb


async def has_field(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: rpb2.FieldID,
) -> bool:
    """Return True if a field row exists, False otherwise."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT 1 FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND field_type = %(field_type)s AND field_id = %(field_id)s
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": from_proto.field_type_name(field_id.field_type).abbreviation(),
                "field_id": field_id.field,
            },
        )
        return await cur.fetchone() is not None
