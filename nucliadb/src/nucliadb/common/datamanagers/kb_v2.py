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

import dataclasses
from collections.abc import AsyncIterator
from typing import cast

from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb_protos import knowledgebox_pb2, writer_pb2

# ---------------------------------------------------------------------------
# Row model
# ---------------------------------------------------------------------------


@dataclasses.dataclass
class KBRow:
    kbid: str
    slug: str | None
    title: str | None
    shards: bytes | None
    config: bytes | None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _pg(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)


def _row_to_kb(row: tuple) -> KBRow:
    kbid, slug, title, shards, config = row
    return KBRow(
        kbid=str(kbid),
        slug=slug,
        title=title,
        shards=bytes(shards) if shards is not None else None,
        config=bytes(config) if config is not None else None,
    )


# ---------------------------------------------------------------------------
# Write operations
# ---------------------------------------------------------------------------


async def upsert(
    txn: Transaction,
    *,
    kbid: str,
    slug: str | None = None,
    title: str | None = None,
    shards: writer_pb2.Shards | None = None,
    config: knowledgebox_pb2.KnowledgeBoxConfig | None = None,
) -> None:
    """Insert or fully replace a KB row."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            """
            INSERT INTO kbs (kbid, slug, title, shards, config)
            VALUES (%(kbid)s, %(slug)s, %(title)s, %(shards)s, %(config)s)
            ON CONFLICT (kbid) DO UPDATE SET
                slug   = EXCLUDED.slug,
                title  = EXCLUDED.title,
                shards = EXCLUDED.shards,
                config = EXCLUDED.config
            """,
            {
                "kbid": kbid,
                "slug": slug,
                "title": title,
                "shards": shards.SerializeToString() if shards is not None else None,
                "config": config.SerializeToString() if config is not None else None,
            },
        )


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


async def set_shards(
    txn: Transaction,
    *,
    kbid: str,
    shards: writer_pb2.Shards,
) -> None:
    """Update only the shards column of an existing KB row."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            """
            UPDATE kbs SET shards = %(shards)s
            WHERE kbid = %(kbid)s
            """,
            {"kbid": kbid, "shards": shards.SerializeToString()},
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

_SELECT_COLUMNS = "kbid, slug, title, shards, config"


async def get(txn: Transaction, *, kbid: str) -> KBRow | None:
    """Return the row for a single KB, or None if it does not exist."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLUMNS} FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        row = await cur.fetchone()
        return _row_to_kb(row) if row is not None else None


async def exists(txn: Transaction, *, kbid: str) -> bool:
    """Return True if a KB with the given kbid exists."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            "SELECT 1 FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )
        return await cur.fetchone() is not None


async def get_by_slug(txn: Transaction, *, slug: str) -> KBRow | None:
    """Return the KB row matching the given slug, or None."""
    async with _pg(txn).connection.cursor() as cur:
        await cur.execute(
            f"SELECT {_SELECT_COLUMNS} FROM kbs WHERE slug = %(slug)s",
            {"slug": slug},
        )
        row = await cur.fetchone()
        return _row_to_kb(row) if row is not None else None


async def get_config(txn: Transaction, *, kbid: str) -> knowledgebox_pb2.KnowledgeBoxConfig | None:
    """Return the deserialised KnowledgeBoxConfig for a KB, or None."""
    row = await get(txn, kbid=kbid)
    if row is None or row.config is None:
        return None
    pb = knowledgebox_pb2.KnowledgeBoxConfig()
    pb.ParseFromString(row.config)
    return pb


async def get_shards(txn: Transaction, *, kbid: str) -> writer_pb2.Shards | None:
    """Return the deserialised Shards for a KB, or None."""
    row = await get(txn, kbid=kbid)
    if row is None or row.shards is None:
        return None
    pb = writer_pb2.Shards()
    pb.ParseFromString(row.shards)
    return pb


async def iter_kbs(txn: Transaction, *, slug_prefix: str = "") -> AsyncIterator[KBRow]:
    """Iterate over all KB rows, optionally filtering by slug prefix."""
    async with _pg(txn).connection.cursor() as cur:
        if slug_prefix:
            await cur.execute(
                f"SELECT {_SELECT_COLUMNS} FROM kbs WHERE slug LIKE %(prefix)s ORDER BY kbid",
                {"prefix": slug_prefix + "%"},
            )
        else:
            await cur.execute(
                f"SELECT {_SELECT_COLUMNS} FROM kbs ORDER BY kbid",
            )
        async for row in cur:
            yield _row_to_kb(row)
