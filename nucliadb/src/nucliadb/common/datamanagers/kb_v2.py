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
Datamanager for the `kbs` PostgreSQL table (migration 0016).

Each row represents one knowledge box and stores:
  - kbid    - primary key
  - slug    - human-readable unique identifier (nullable)
  - title   - display name (nullable)
  - shards  - serialised writer_pb2.Shards protobuf
  - config  - serialised knowledgebox_pb2.KnowledgeBoxConfig protobuf
"""

from collections.abc import AsyncIterator
from typing import cast

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb_protos import knowledgebox_pb2, writer_pb2

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pg(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


async def set_config(
    txn: Transaction,
    *,
    kbid: str,
    config: knowledgebox_pb2.KnowledgeBoxConfig,
) -> None:
    """Update only the config column of an existing KB row."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            """
            UPDATE kbs SET config = %(config)s
            WHERE kbid = %(kbid)s
            """,
            {"kbid": kbid, "config": config.SerializeToString()},
        )


async def delete(txn: Transaction, *, kbid: str) -> None:
    """Delete a KB row (cascades to kb_resources and fields)."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            "DELETE FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


async def exists_kb(txn: Transaction, *, kbid: str) -> bool:
    """Return True if a KB with the given kbid exists."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        return await cur.fetchone() is not None


async def get_config(txn: Transaction, *, kbid: str) -> knowledgebox_pb2.KnowledgeBoxConfig | None:
    """Return the deserialised KnowledgeBoxConfig for a KB, or None."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            "SELECT config FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        row = await cur.fetchone()
        if row is None or row[0] is None:
            return None
        pb = knowledgebox_pb2.KnowledgeBoxConfig()
        pb.ParseFromString(row[0])
        return pb


async def set_kbid_for_slug(txn: Transaction, *, slug: str, kbid: str) -> None:
    """Set the slug for a given kbid, overwriting any existing slug. This is used when migrating from the old slug-based system to the new kbid-based system."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            """
            UPDATE kbs SET slug = %(slug)s
            WHERE kbid = %(kbid)s
            """,
            {"kbid": kbid, "slug": slug},
        )


async def get_kb_uuid(txn: Transaction, *, slug: str) -> str | None:
    """Return the kbid for a given slug, or None if it does not exist."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            "SELECT kbid FROM kbs WHERE slug = %(slug)s",
            {"slug": slug},
        )
        row = await cur.fetchone()
        return str(row[0]) if row is not None else None


async def get_kbs(txn: Transaction, *, slug_prefix: str = "") -> AsyncIterator[tuple[str, str]]:
    """Iterate over all KBs, yielding (kbid, slug) tuples, optionally filtering by slug prefix."""
    async with _pg(txn).connection.cursor() as cur:
        if slug_prefix:
            await cur.execute(
                "SELECT kbid, slug FROM kbs WHERE slug LIKE %(prefix)s ORDER BY kbid",
                {"prefix": slug_prefix + "%"},
            )
        else:
            await cur.execute(
                "SELECT kbid, slug FROM kbs ORDER BY kbid",
            )
        async for row in cur:
            yield (str(row[0]), row[1])


async def update_kb_shards(
    txn: Transaction,
    *,
    kbid: str,
    shards: writer_pb2.Shards,
) -> None:
    """Update the shards column of a KB row."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            """
            UPDATE kbs SET shards = %(shards)s
            WHERE kbid = %(kbid)s
            """,
            {"kbid": kbid, "shards": shards.SerializeToString()},
        )


async def get_kb_shards(
    txn: Transaction,
    *,
    kbid: str,
    for_update: bool = False,
) -> writer_pb2.Shards | None:
    """Return the deserialised Shards for a KB, or None."""
    async with _pg(txn).connection.cursor() as cur:
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
