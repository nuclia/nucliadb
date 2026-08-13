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

from dataclasses import dataclass
from typing import Final, Literal, Sequence, TypeAlias, cast

import psycopg.sql
from google.protobuf.message import Message

from nucliadb.common.datamanagers.utils import UNSET, _pg_cursor, _UnsetType, observer
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.models_utils import from_proto, to_proto
from nucliadb_protos import resources_pb2 as rpb2
from nucliadb_protos import writer_pb2 as wpb2

FieldColumn: TypeAlias = Literal["status", "value", "md5"]

UNSET_STATUS: Final[wpb2.FieldStatus | None] = cast(wpb2.FieldStatus | None, UNSET)
UNSET_VALUE: Final[bytes | None] = cast(bytes | None, UNSET)
UNSET_MD5: Final[str | None] = cast(str | None, UNSET)


@dataclass(slots=True)
class FieldData:
    status: wpb2.FieldStatus | None = UNSET_STATUS
    value: bytes | None = UNSET_VALUE
    md5: str | None = UNSET_MD5


def _serialize_field_column(value):
    if value is UNSET:
        return UNSET
    if value is None:
        return None
    if isinstance(value, Message):
        return value.SerializeToString()
    return value


def _deserialize_field_column(column: FieldColumn, value):
    if value is None:
        return None
    if column == "status":
        pb = wpb2.FieldStatus()
        pb.ParseFromString(bytes(value))
        return pb
    if column == "value":
        return bytes(value)
    return str(value)


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "field", "op": "set_status"})
async def set_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: wpb2.FieldStatus,
) -> None:
    await set(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        status=status,
    )


@observer.wrap({"type": "field", "op": "set_statuses"})
async def set_statuses(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    statuses: Sequence[tuple[str, str, wpb2.FieldStatus]],
) -> None:
    if not statuses:
        return

    values_sql = ",".join(["(%s, %s, %s, %s, %s)"] * len(statuses))
    values: list[str | bytes] = []
    for status_field_type, status_field_id, status in statuses:
        values.extend((kbid, rid, status_field_type, status_field_id, status.SerializeToString()))

    async with _pg_cursor(txn) as cur:
        await cur.execute(
            f"""
            INSERT INTO kb_fields (kbid, rid, field_type, field_id, status)
            VALUES {values_sql}
            ON CONFLICT (kbid, rid, field_type, field_id) DO UPDATE SET
                status = EXCLUDED.status
            """,
            values,
        )


@observer.wrap({"type": "field", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: wpb2.FieldStatus | bytes | None | _UnsetType = UNSET,
    value: Message | bytes | None | _UnsetType = UNSET,
    md5: str | None | _UnsetType = UNSET,
) -> None:
    return await _set(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        status=status,
        value=value,
        md5=md5,
    )


async def _set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    status: wpb2.FieldStatus | bytes | None | _UnsetType = UNSET,
    value: Message | bytes | None | _UnsetType = UNSET,
    md5: str | None | _UnsetType = UNSET,
) -> None:
    values = {
        "kbid": kbid,
        "rid": rid,
        "field_type": field_type,
        "field_id": field_id,
        "status": _serialize_field_column(status),
        "value": _serialize_field_column(value),
        "md5": _serialize_field_column(md5),
    }
    columns_to_set = [
        column_name for column_name in ("status", "value", "md5") if values[column_name] is not UNSET
    ]
    if not columns_to_set:
        return

    insert_columns = ["kbid", "rid", "field_type", "field_id", *columns_to_set]
    assignments = [
        psycopg.sql.SQL("{} = EXCLUDED.{}").format(
            psycopg.sql.Identifier(column_name),
            psycopg.sql.Identifier(column_name),
        )
        for column_name in columns_to_set
    ]

    query = psycopg.sql.SQL(
        """
        INSERT INTO kb_fields ({insert_columns})
        VALUES ({insert_values})
        ON CONFLICT (kbid, rid, field_type, field_id) DO UPDATE SET
            {assignments}
        """
    ).format(
        insert_columns=psycopg.sql.SQL(", ").join(
            psycopg.sql.Identifier(column_name) for column_name in insert_columns
        ),
        insert_values=psycopg.sql.SQL(", ").join(
            psycopg.sql.Placeholder(column_name) for column_name in insert_columns
        ),
        assignments=psycopg.sql.SQL(", ").join(assignments),
    )

    async with _pg_cursor(txn) as cur:
        await cur.execute(query, {column_name: values[column_name] for column_name in insert_columns})


@observer.wrap({"type": "field", "op": "delete"})
async def delete(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> None:
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


@observer.wrap({"type": "field", "op": "get_raw"})
async def get_raw(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> bytes | None:
    field = await _get(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        columns=("value",),
    )
    if field is None:
        return None
    assert field.value is not UNSET
    return field.value


@observer.wrap({"type": "field", "op": "get_status"})
async def get_status(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
) -> wpb2.FieldStatus | None:
    field = await _get(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        columns=("status",),
    )
    if field is None:
        return None
    assert field.status is not UNSET
    return field.status


@observer.wrap({"type": "field", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    columns: tuple[FieldColumn, ...],
    for_update: bool = False,
) -> FieldData | None:
    return await _get(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        columns=columns,
        for_update=for_update,
    )


async def _get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_type: str,
    field_id: str,
    columns: tuple[FieldColumn, ...],
    for_update: bool = False,
) -> FieldData | None:
    if not columns:
        raise ValueError("At least one field column must be requested")

    query = psycopg.sql.SQL(
        """
        SELECT {columns} FROM kb_fields
        WHERE kbid = %(kbid)s AND rid = %(rid)s
          AND field_type = %(field_type)s AND field_id = %(field_id)s
        """
    ).format(
        columns=psycopg.sql.SQL(", ").join(
            psycopg.sql.Identifier(column_name) for column_name in columns
        )
    )
    if for_update:
        query += psycopg.sql.SQL(" FOR UPDATE")

    async with _pg_cursor(txn) as cur:
        await cur.execute(
            query,
            {
                "kbid": kbid,
                "rid": rid,
                "field_type": field_type,
                "field_id": field_id,
            },
        )
        row = await cur.fetchone()
        if row is None:
            return None

    field = FieldData()
    for index, column_name in enumerate(columns):
        setattr(field, column_name, _deserialize_field_column(column_name, row[index]))
    return field


@observer.wrap({"type": "field", "op": "get_statuses"})
async def get_statuses(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    fields: Sequence[rpb2.FieldID],
) -> list[wpb2.FieldStatus]:
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
            [kbid, rid] + [item for f in fields for item in (_to_abbr(f.field_type), f.field)],
        )
        rows = await cur.fetchall()

    # Build a lookup dict for fast access
    status_lookup = {(row[0], row[1]): bytes(row[2]) if row[2] is not None else None for row in rows}

    result = []
    for f in fields:
        status_bytes = status_lookup.get((_to_abbr(f.field_type), f.field))
        if status_bytes is None:
            result.append(wpb2.FieldStatus())  # Default empty status
        else:
            pb = wpb2.FieldStatus()
            pb.ParseFromString(status_bytes)
            result.append(pb)

    return result


def _to_abbr(field_type: rpb2.FieldType.ValueType) -> str:
    """Convert a FieldType enum to its single-character abbreviation."""
    return from_proto.field_type_name(field_type).abbreviation()


@observer.wrap({"type": "field", "op": "get_all_field_ids"})
async def get_all_field_ids(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
) -> rpb2.AllFieldIDs:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT field_type, field_id FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = %(rid)s
              AND NOT (field_type = 'a' AND field_id IN ('title', 'summary'))
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


@observer.wrap({"type": "field", "op": "exists"})
async def exists(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    field_id: rpb2.FieldID,
) -> bool:
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


@observer.wrap({"type": "field", "op": "exists_md5"})
async def exists_md5(
    txn: Transaction,
    *,
    kbid: str,
    md5: str,
    field_type: str,
) -> bool:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT 1 FROM kb_fields WHERE kbid = %(kbid)s AND md5 = %(md5)s AND field_type = %(field_type)s LIMIT 1",
            {"kbid": kbid, "md5": md5, "field_type": field_type},
        )
        return await cur.fetchone() is not None


@observer.wrap({"type": "field", "op": "set_md5"})
async def set_md5(
    txn: Transaction, *, kbid: str, md5: str, rid: str, field_id: str, field_type: str
) -> None:
    await set(
        txn,
        kbid=kbid,
        rid=rid,
        field_type=field_type,
        field_id=field_id,
        md5=md5,
    )
