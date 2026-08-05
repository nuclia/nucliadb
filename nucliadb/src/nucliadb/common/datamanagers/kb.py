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

import logging
from collections.abc import AsyncIterator

import psycopg.errors

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.datamanagers.utils import _pg_cursor
from nucliadb.common.maindb.driver import Transaction
from nucliadb_protos import knowledgebox_pb2

from . import cluster

logger = logging.getLogger(__name__)


async def get_kbs(txn: Transaction, *, slug_prefix: str = "") -> AsyncIterator[tuple[str, str]]:
    async with _pg_cursor(txn) as cur:
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
            if row[1] is not None:  # Only yield KBs that have a slug (i.e., not soft-deleted)
                yield (str(row[0]), row[1])


async def exists_kb(txn: Transaction, *, kbid: str) -> bool:
    async with _pg_cursor(txn) as cur:
        try:
            await cur.execute(
                """
                SELECT 1 FROM kbs
                WHERE kbid = %(kbid)s
                  AND slug IS NOT NULL
                  AND deleted_at IS NULL
                """,
                {"kbid": kbid},
            )
        except psycopg.errors.InvalidTextRepresentation:
            logger.warning(
                "Invalid UUID format in exists_kb() check, returning False",
                extra={"kbid": kbid},
            )
            return False
        return await cur.fetchone() is not None


async def get_kb_uuid(txn: Transaction, *, slug: str) -> str | None:
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "SELECT kbid FROM kbs WHERE slug = %(slug)s",
            {"slug": slug},
        )
        row = await cur.fetchone()
        return str(row[0]) if row is not None else None


async def set_kbid_for_slug(txn: Transaction, *, slug: str, kbid: str):
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kbs (kbid, slug)
            VALUES (%(kbid)s, %(slug)s)
            ON CONFLICT (kbid) DO UPDATE SET
                slug = EXCLUDED.slug
            """,
            {"kbid": kbid, "slug": slug},
        )


async def delete(txn: Transaction, *, kbid: str) -> None:
    """Fully delete a KB row and all its associated resources, fields, and conversations."""
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "DELETE FROM kbs WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )


async def soft_delete(txn: Transaction, *, kbid: str) -> None:
    """Soft delete a KB row by clearing its slug and stamping deleted_at with the current time.

    No-op if the KB does not exist (UPDATE affects 0 rows without raising an error).
    """
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            "UPDATE kbs SET slug = NULL, deleted_at = NOW() WHERE kbid = %(kbid)s",
            {"kbid": kbid},
        )


async def get_config(
    txn: Transaction, *, kbid: str, for_update: bool = False
) -> knowledgebox_pb2.KnowledgeBoxConfig | None:
    async with _pg_cursor(txn) as cur:
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


async def set_config(txn: Transaction, *, kbid: str, config: knowledgebox_pb2.KnowledgeBoxConfig):
    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kbs (kbid, config)
            VALUES (%(kbid)s, %(config)s)
            ON CONFLICT (kbid) DO UPDATE SET
                config = EXCLUDED.config
            """,
            {"kbid": kbid, "config": config.SerializeToString()},
        )


async def get_model_metadata(txn: Transaction, *, kbid: str) -> knowledgebox_pb2.SemanticModelMetadata:
    shards_obj = await cluster.get_kb_shards(txn, kbid=kbid, for_update=False)
    if shards_obj is None:
        raise KnowledgeBoxNotFound(kbid)
    if shards_obj.HasField("model"):
        return shards_obj.model
    else:
        # B/c code for old KBs that do not have the `model` attribute set in the Shards object.
        # Cleanup this code after a migration is done unifying all fields under `model` (on-prem and cloud).
        return knowledgebox_pb2.SemanticModelMetadata(similarity_function=shards_obj.similarity)


# DEPRECATED: this function should be removed once the "default" vectorset
# concept is removed and processing sends us all messages with a vectorset_id
async def get_matryoshka_vector_dimension(
    txn: Transaction,
    *,
    kbid: str,
    vectorset_id: str | None = None,
) -> int | None:
    """Return vector dimension for matryoshka models"""
    from . import vectorsets

    async for _, vs in vectorsets.iter(txn, kbid=kbid):
        if len(vs.matryoshka_dimensions) > 0 and vs.vectorset_index_config.vector_dimension:
            if vs.vectorset_index_config.vector_dimension in vs.matryoshka_dimensions:
                return vs.vectorset_index_config.vector_dimension
            else:
                logger.error(
                    "KB has an invalid matryoshka dimension!",
                    extra={
                        "kbid": kbid,
                        "vector_dimension": vs.vectorset_index_config.vector_dimension,
                        "matryoshka_dimensions": vs.matryoshka_dimensions,
                    },
                )
        return None
    else:
        # fallback for KBs that don't have vectorset
        model_metadata = await get_model_metadata(txn, kbid=kbid)
        dimension = None
        if len(model_metadata.matryoshka_dimensions) > 0 and model_metadata.vector_dimension:
            if model_metadata.vector_dimension in model_metadata.matryoshka_dimensions:
                dimension = model_metadata.vector_dimension
            else:
                logger.error(
                    "KB has an invalid matryoshka dimension!",
                    extra={
                        "kbid": kbid,
                        "vector_dimension": model_metadata.vector_dimension,
                        "matryoshka_dimensions": model_metadata.matryoshka_dimensions,
                    },
                )
        return dimension


async def get_external_index_provider_metadata(
    txn: Transaction, *, kbid: str
) -> knowledgebox_pb2.StoredExternalIndexProviderMetadata | None:
    kb_config = await get_config(txn, kbid=kbid)
    if kb_config is None:
        return None
    return kb_config.external_index_provider


async def set_external_index_provider_metadata(
    txn: Transaction, *, kbid: str, metadata: knowledgebox_pb2.StoredExternalIndexProviderMetadata
):
    kb_config = await get_config(txn, kbid=kbid)
    if kb_config is None:
        raise KnowledgeBoxNotFound(kbid)
    kb_config.external_index_provider.CopyFrom(metadata)
    await set_config(txn, kbid=kbid, config=kb_config)
