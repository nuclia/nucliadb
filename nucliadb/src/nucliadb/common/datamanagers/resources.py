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
Datamanager for the `kb_resources` PostgreSQL table (migration 0016).

Each row represents one resource in a knowledge box and stores:
  - kbid           - FK → kbs.kbid (ON DELETE RESTRICT)
  - rid            - resource UUID
  - slug           - optional human-readable identifier
  - shard          - shard ID the resource belongs to
  - basic          - serialised resources_pb2.Basic
  - origin         - serialised resources_pb2.Origin
  - security       - serialised resources_pb2.Security
  - extra          - serialised resources_pb2.Extra
"""

import logging
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, Literal, TypeAlias, cast

import psycopg.errors
import psycopg.sql
from typing_extensions import assert_never

from nucliadb.common.datamanagers.utils import (
    UNSET,
    _pg_cursor,
    _UnsetType,
    observer,
    with_ro_transaction,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb_protos import resources_pb2

logger = logging.getLogger(__name__)


ResourceColumn: TypeAlias = Literal["slug", "shard", "basic", "origin", "security", "extra"]

UNSET_STR: Final[str | None] = cast(str | None, UNSET)
UNSET_BASIC: Final[resources_pb2.Basic | None] = cast(resources_pb2.Basic | None, UNSET)
UNSET_ORIGIN: Final[resources_pb2.Origin | None] = cast(resources_pb2.Origin | None, UNSET)
UNSET_SECURITY: Final[resources_pb2.Security | None] = cast(resources_pb2.Security | None, UNSET)
UNSET_EXTRA: Final[resources_pb2.Extra | None] = cast(resources_pb2.Extra | None, UNSET)


@dataclass(slots=True)
class ResourceData:
    slug: str | None = UNSET_STR
    shard: str | None = UNSET_STR
    basic: resources_pb2.Basic | None = UNSET_BASIC
    origin: resources_pb2.Origin | None = UNSET_ORIGIN
    security: resources_pb2.Security | None = UNSET_SECURITY
    extra: resources_pb2.Extra | None = UNSET_EXTRA


ResourceColumnValueType = (
    str
    | None
    | resources_pb2.Basic
    | resources_pb2.Origin
    | resources_pb2.Security
    | resources_pb2.Extra
)
SerializedResourceColumnValueType = str | None | bytes


def _serialize_resource_column(
    value: _UnsetType | ResourceColumnValueType,
) -> _UnsetType | SerializedResourceColumnValueType:
    if isinstance(value, _UnsetType):
        return UNSET
    elif value is None:
        return None
    elif isinstance(value, str):
        return value
    elif isinstance(value, resources_pb2.Basic):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Origin):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Security):
        return value.SerializeToString()
    elif isinstance(value, resources_pb2.Extra):
        return value.SerializeToString()
    else:  # pragma: no cover
        assert_never(value)


def _deserialize_resource_column(
    column: ResourceColumn, value: SerializedResourceColumnValueType
) -> ResourceColumnValueType:
    if value is None:
        return None
    if column == "slug":
        return str(value)
    elif column == "shard":
        return str(value)
    elif column == "basic":
        assert isinstance(value, bytes)
        pb = resources_pb2.Basic()
        pb.ParseFromString(value)
        return pb
    elif column == "origin":
        assert isinstance(value, bytes)
        pb_origin = resources_pb2.Origin()
        pb_origin.ParseFromString(value)
        return pb_origin
    elif column == "security":
        assert isinstance(value, bytes)
        pb_security = resources_pb2.Security()
        pb_security.ParseFromString(value)
        return pb_security
    elif column == "extra":
        assert isinstance(value, bytes)
        pb_extra = resources_pb2.Extra()
        pb_extra.ParseFromString(value)
        return pb_extra
    else:  # pragma: no cover
        assert_never(column)


def _to_rid(value: uuid.UUID) -> str:
    """Return the 32-char hex form (no hyphens) of a UUID column value."""
    return value.hex


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "set"})
async def set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    shard: str | None | _UnsetType = UNSET,
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
) -> None:
    return await _set(
        txn,
        kbid=kbid,
        rid=rid,
        shard=shard,
        basic=basic,
        origin=origin,
        security=security,
        extra=extra,
    )


async def _set(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str | None | _UnsetType = UNSET,
    shard: str | None | _UnsetType = UNSET,
    basic: resources_pb2.Basic | None | _UnsetType = UNSET,
    origin: resources_pb2.Origin | None | _UnsetType = UNSET,
    security: resources_pb2.Security | None | _UnsetType = UNSET,
    extra: resources_pb2.Extra | None | _UnsetType = UNSET,
) -> None:
    """Upsert a resource row, updating only columns explicitly provided.
    Use UNSET to leave a column untouched. Pass None to explicitly store SQL NULL.
    """
    values = {
        "kbid": kbid,
        "rid": rid,
        "slug": _serialize_resource_column(slug),
        "shard": _serialize_resource_column(shard),
        "basic": _serialize_resource_column(basic),
        "origin": _serialize_resource_column(origin),
        "security": _serialize_resource_column(security),
        "extra": _serialize_resource_column(extra),
    }
    columns_to_set = [
        column_name
        for column_name in ("slug", "shard", "basic", "origin", "security", "extra")
        if values[column_name] is not UNSET
    ]
    if not columns_to_set:
        return

    insert_columns = ["kbid", "rid", *columns_to_set]
    assignments = [
        psycopg.sql.SQL("{} = EXCLUDED.{}").format(
            psycopg.sql.Identifier(column_name),
            psycopg.sql.Identifier(column_name),
        )
        for column_name in columns_to_set
    ]

    query = psycopg.sql.SQL(
        """
        INSERT INTO kb_resources ({insert_columns})
        VALUES ({insert_values})
        ON CONFLICT (kbid, rid) DO UPDATE SET
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


@observer.wrap({"type": "resources", "op": "set_slug"})
async def set_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str,
) -> None:
    try:
        await _set(txn, kbid=kbid, rid=rid, slug=slug)
    except psycopg.errors.UniqueViolation:
        raise ConflictError(f"Slug '{slug}' already exists")


@observer.wrap({"type": "resources", "op": "update_slug"})
async def update_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    new_slug: str,
) -> str:
    """
    NOTE: Slug is stored twice (in the slug column and in the basic column).
    This function makes sure to update both in a single transaction.
    Ideally we should only store it in the slug column.
    """
    data = await _get(txn, kbid=kbid, rid=rid, columns=("basic", "slug"), for_update=True)
    if (
        data is None
        or data.basic is None
        or data.basic is UNSET
        or data.slug is None
        or data.slug is UNSET
    ):
        raise NotFoundError()
    old_slug = data.slug
    basic = data.basic
    basic.slug = new_slug
    try:
        await _set(txn, kbid=kbid, rid=rid, slug=new_slug, basic=basic)
    except psycopg.errors.UniqueViolation:
        raise ConflictError(f"Slug '{new_slug}' already exists")
    return old_slug


@observer.wrap({"type": "resources", "op": "delete"})
async def delete(txn: Transaction, *, kbid: str, rid: str) -> None:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "DELETE FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@observer.wrap({"type": "resources", "op": "exists"})
async def exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    async with _pg_cursor(txn) as cur:
        try:
            await cur.execute(
                "SELECT 1 FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
                {"kbid": kbid, "rid": rid},
            )
        except psycopg.errors.InvalidTextRepresentation:
            logger.warning(
                "Invalid UUID format in exists() check, returning False",
                extra={"kbid": kbid, "rid": rid},
            )
            return False
        return await cur.fetchone() is not None


@observer.wrap({"type": "resources", "op": "get_rid"})
async def get_rid(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT rid FROM kb_resources WHERE kbid = %(kbid)s AND slug = %(slug)s",
            {"kbid": kbid, "slug": slug},
        )
        row = await cur.fetchone()
        return _to_rid(row[0]) if row is not None else None


@observer.wrap({"type": "resources", "op": "slug_exists"})
async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT 1 FROM kb_resources WHERE kbid = %(kbid)s AND slug = %(slug)s",
            {"kbid": kbid, "slug": slug},
        )
        return await cur.fetchone() is not None


@observer.wrap({"type": "resources", "op": "get_basic"})
async def get_basic(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> resources_pb2.Basic | None:
    async with _pg_cursor(txn) as cur:
        statement = "SELECT basic FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s"
        if for_update:
            statement += " FOR UPDATE"
        await cur.execute(
            statement,
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = resources_pb2.Basic()
        pb.ParseFromString(bytes(row[0]))
        return pb


@observer.wrap({"type": "resources", "op": "iter"})
async def iter(*, kbid: str) -> AsyncIterator[str]:
    async with with_ro_transaction() as txn:
        async with _pg_cursor(txn) as cur:
            await cur.execute(
                "SELECT rid FROM kb_resources WHERE kbid = %(kbid)s ORDER BY rid",
                {"kbid": kbid},
            )
            async for (rid,) in cur:
                yield _to_rid(rid)


@observer.wrap({"type": "resources", "op": "count"})
async def count(txn: Transaction, *, kbid: str) -> int:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT COUNT(*) FROM kb_resources WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        row = await cur.fetchone()
        return row[0] if row else 0


@observer.wrap({"type": "resources", "op": "get_shard"})
async def get_shard(txn: Transaction, *, kbid: str, rid: str, for_update: bool = False) -> str | None:
    resource = await _get(txn, kbid=kbid, rid=rid, columns=("shard",), for_update=for_update)
    if resource is None:
        return None
    assert resource.shard is not UNSET
    return resource.shard


@observer.wrap({"type": "resources", "op": "get_shards"})
async def get_shards(txn: Transaction, *, kbid: str, rids: list[str]) -> dict[str, str]:
    """Return a mapping of rid to shard for the given rids in a single query."""
    result: dict[str, str] = {}
    if not rids:
        return result
    placeholders = [psycopg.sql.Placeholder(f"rid_{i}") for i in range(len(rids))]
    query = psycopg.sql.SQL(
        "SELECT rid, shard FROM kb_resources WHERE kbid = %(kbid)s AND rid IN ({placeholders})"
    ).format(placeholders=psycopg.sql.SQL(", ").join(placeholders))
    params = {"kbid": kbid, **{f"rid_{i}": rid for i, rid in enumerate(rids)}}
    async with _pg_cursor(txn) as cur:
        await cur.execute(query, params)
        async for row in cur:
            rid, shard = row
            result[_to_rid(rid)] = shard
        return result


@observer.wrap({"type": "resources", "op": "get"})
async def get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    """Return the selected resource columns for a row, or None if the row does not exist.

    Non-requested fields are left as UNSET. Requested SQL NULL values are returned as None.
    """
    return await _get(txn, kbid=kbid, rid=rid, columns=columns, for_update=for_update)


async def _get(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    columns: tuple[ResourceColumn, ...],
    for_update: bool = False,
) -> ResourceData | None:
    """Return the selected resource columns for a row, or None if the row does not exist.

    Non-requested fields are left as UNSET. Requested SQL NULL values are returned as None.
    """
    if not columns:
        raise ValueError("At least one resource column must be requested")

    query = psycopg.sql.SQL(
        "SELECT {columns} FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s"
    ).format(
        columns=psycopg.sql.SQL(", ").join(
            psycopg.sql.Identifier(column_name) for column_name in columns
        )
    )
    if for_update:
        query += psycopg.sql.SQL(" FOR UPDATE")

    async with _pg_cursor(txn) as cur:
        await cur.execute(query, {"kbid": kbid, "rid": rid})
        row = await cur.fetchone()
        if row is None:
            return None

    resource = ResourceData()
    for index, column_name in enumerate(columns):
        setattr(resource, column_name, _deserialize_resource_column(column_name, row[index]))
    return resource
