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

import psycopg.errors

from nucliadb.common.datamanagers.utils import (
    _pg_cursor,
    handle_invalid_uuid,
    logs_foreign_key_error,
    with_ro_transaction,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb_protos import resources_pb2

logger = logging.getLogger(__name__)


def _to_rid(value: uuid.UUID) -> str:
    """Return the 32-char hex form (no hyphens) of a UUID column value."""
    return value.hex


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


@logs_foreign_key_error
async def set_basic(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    basic: resources_pb2.Basic,
) -> None:
    """Upsert the basic column of a resource row, creating the row if it does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, basic)
            VALUES (%(kbid)s, %(rid)s, %(basic)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                basic = EXCLUDED.basic
            """,
            {"kbid": kbid, "rid": rid, "basic": basic.SerializeToString()},
        )


@logs_foreign_key_error
async def set_origin(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    origin: resources_pb2.Origin,
) -> None:
    """Upsert the origin column, creating the row if it does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, origin)
            VALUES (%(kbid)s, %(rid)s, %(origin)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                origin = EXCLUDED.origin
            """,
            {"kbid": kbid, "rid": rid, "origin": origin.SerializeToString()},
        )


@logs_foreign_key_error
async def set_security(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    security: resources_pb2.Security,
) -> None:
    """Upsert the security column, creating the row if it does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, security)
            VALUES (%(kbid)s, %(rid)s, %(security)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                security = EXCLUDED.security
            """,
            {"kbid": kbid, "rid": rid, "security": security.SerializeToString()},
        )


@logs_foreign_key_error
async def set_extra(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    extra: resources_pb2.Extra,
) -> None:
    """Upsert the extra column, creating the row if it does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, extra)
            VALUES (%(kbid)s, %(rid)s, %(extra)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                extra = EXCLUDED.extra
            """,
            {"kbid": kbid, "rid": rid, "extra": extra.SerializeToString()},
        )


async def get_slug(txn: Transaction, kbid: str, rid: str) -> str | None:
    """Get the slug of a resource."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            SELECT slug FROM kb_resources
            WHERE kbid = %(kbid)s AND rid = %(rid)s
            """,
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        return str(row[0]) if row is not None else None


@logs_foreign_key_error
async def set_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    slug: str,
) -> None:
    """Update only the slug column of an existing resource row.

    Raises ConflictError if the slug already belongs to another resource in
    the same knowledge box.
    """
    async with _pg_cursor(txn) as cur:
        try:
            await cur.execute(
                """
                INSERT INTO kb_resources (kbid, rid, slug)
                VALUES (%(kbid)s, %(rid)s, %(slug)s)
                ON CONFLICT (kbid, rid) DO UPDATE SET
                    slug = EXCLUDED.slug
                """,
                {"kbid": kbid, "rid": rid, "slug": slug},
            )
        except psycopg.errors.UniqueViolation:
            raise ConflictError(f"Slug '{slug}' already exists")


@logs_foreign_key_error
async def modify_slug(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    new_slug: str,
) -> str:
    """Update only the slug column of an existing resource row.

    Returns the old slug value.
    Raises NotFoundError if the resource does not exist.
    Raises ConflictError if the slug already belongs to another resource in
    the same knowledge box.
    """
    old_slug = await get_slug(txn, kbid=kbid, rid=rid)
    if old_slug is None:
        raise NotFoundError()
    async with _pg_cursor(txn) as cur:
        try:
            await cur.execute(
                """
                UPDATE kb_resources SET slug = %(slug)s
                WHERE kbid = %(kbid)s AND rid = %(rid)s
                """,
                {"kbid": kbid, "rid": rid, "slug": new_slug},
            )
            return old_slug
        except psycopg.errors.UniqueViolation:
            raise ConflictError(f"Slug '{new_slug}' already exists")


@logs_foreign_key_error
async def set_resource_shard_id(
    txn: Transaction,
    *,
    kbid: str,
    rid: str,
    shard: str,
) -> None:
    """Upsert the shard column, creating the row if it does not exist."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, shard)
            VALUES (%(kbid)s, %(rid)s, %(shard)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                shard = EXCLUDED.shard
            """,
            {"kbid": kbid, "rid": rid, "shard": shard},
        )


async def delete(txn: Transaction, *, kbid: str, rid: str) -> None:
    """Delete a resource row (cascades to fields)."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "DELETE FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


@handle_invalid_uuid(default=False)
async def exists(txn: Transaction, *, kbid: str, rid: str) -> bool:
    """Return True if the resource exists."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT 1 FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )
        return await cur.fetchone() is not None


@handle_invalid_uuid(default=None)
async def get_resource_uuid_from_slug(txn: Transaction, *, kbid: str, slug: str) -> str | None:
    """Return the resource UUID for the given slug within a KB, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT rid FROM kb_resources WHERE kbid = %(kbid)s AND slug = %(slug)s",
            {"kbid": kbid, "slug": slug},
        )
        row = await cur.fetchone()
        return _to_rid(row[0]) if row is not None else None


@handle_invalid_uuid(default=False)
async def slug_exists(txn: Transaction, *, kbid: str, slug: str) -> bool:
    """Return True if a resource with the given slug exists within a KB."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT 1 FROM kb_resources WHERE kbid = %(kbid)s AND slug = %(slug)s",
            {"kbid": kbid, "slug": slug},
        )
        return await cur.fetchone() is not None


@handle_invalid_uuid(default=None)
async def get_basic(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Basic | None:
    """Return the deserialised Basic for a resource, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT slug, basic FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        if row is None or row[1] is None:
            return None
        slug = row[0]
        pb = resources_pb2.Basic()
        pb.ParseFromString(bytes(row[1]))
        pb.slug = slug
        return pb


async def get_origin(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Origin | None:
    """Return the deserialised Origin for a resource, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT origin FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = resources_pb2.Origin()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def get_security(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Security | None:
    """Return the deserialised Security for a resource, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT security FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = resources_pb2.Security()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def get_extra(txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Extra | None:
    """Return the deserialised Extra for a resource, or None."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT extra FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s",
            {"kbid": kbid, "rid": rid},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = resources_pb2.Extra()
        pb.ParseFromString(bytes(row[0]))
        return pb


async def iterate_resource_ids(*, kbid: str) -> AsyncIterator[str]:
    """Iterate over all resource UUIDs in a knowledge box."""
    async with with_ro_transaction() as txn:
        async with _pg_cursor(txn) as cur:
            await cur.execute(
                "SELECT rid FROM kb_resources WHERE kbid = %(kbid)s ORDER BY rid",
                {"kbid": kbid},
            )
            async for (rid,) in cur:
                yield _to_rid(rid)


async def calculate_number_of_resources(txn: Transaction, *, kbid: str) -> int:
    """Return the total number of resources in a knowledge box."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT COUNT(*) FROM kb_resources WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        row = await cur.fetchone()
        return row[0] if row else 0


async def get_resource_shard_id(
    txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
) -> str | None:
    """Return the shard ID for a resource, or None."""
    async with _pg_cursor(txn) as cur:
        sql = "SELECT shard FROM kb_resources WHERE kbid = %(kbid)s AND rid = %(rid)s"
        if for_update:
            sql += " FOR UPDATE"
        await cur.execute(sql, {"kbid": kbid, "rid": rid})
        row = await cur.fetchone()
        return row[0] if row is not None else None
