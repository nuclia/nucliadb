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
One-off migration of the metadata stored in the legacy PostgreSQL maindb into MarkLogic.

Migrated data (read from PostgreSQL):
  - `kbs`              -> KB registry (system database) + KB data document (KB database)
  - `resources` (k/v)  -> KB scoped maindb keys (labels, vectorsets, synonyms, ...)
  - `kb_resources`     -> resource documents
  - `kb_fields`        -> field documents (value, status, md5)
  - `kb_conversations` -> conversation pages and splits metadata

Processed fields, chunks and embeddings are rebuilt from the existing object-store
extracted data using the current indexing pipeline. No processing API calls are made.

The migration writes each resource and its children atomically and skips existing resources.
Stop writers/ingest while migrating and keep PostgreSQL and object storage unchanged.

Usage:
    python pg_to_marklogic.py --pg-url postgresql://... [--kbid KBID ...]
    python pg_to_marklogic.py --skip-kb-metadata --pg-url postgresql://...
    python pg_to_marklogic.py --skip-kb-metadata --kbid KBID --rid RID

MarkLogic and object storage use the usual service environment settings. The object
store must be the one used by the source PostgreSQL deployment.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import logging
import os
import sys
import uuid
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

import backoff
import psycopg
from google.protobuf.message import Message

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.conversations import ConversationPageContent
from nucliadb.common.datamanagers.fields import FieldContent
from nucliadb.common.datamanagers.resources import ResourceCreateContent
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.common.marklogic import documents, vector_views
from nucliadb.common.marklogic.client import Document
from nucliadb.common.marklogic.exceptions import MarkLogicResponseError
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.ingest.orm.processor.processor_marklogic import (
    IndexMessageBuilderV2,
    get_field_processed_contents,
)
from nucliadb.ingest.orm.processor.sequence_manager import TXNID
from nucliadb.ingest.orm.resource import Resource
from nucliadb_protos import knowledgebox_pb2, resources_pb2, writer_pb2
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage, teardown_storage

logger = logging.getLogger("pg_to_marklogic")

# KB scoped maindb keys still in use by the MarkLogic datamanagers. Legacy keys from the
# pre-ORM layout (/r/, /s/, /shards, /config, ...) and rollover state are intentionally skipped.
KB_KV_EXACT_KEYS = (
    "labels",
    "ids-labels",
    "vectorsets",
    "vectorsets_node",
    "vectorsets_edge",
    "synonyms",
    "kv_schemas",
)
KB_KV_PREFIX_KEYS = (
    "labels/",
    "search_configuration/",
)

FIELD_VALUE_PB: dict[str, type[Message]] = {
    "t": resources_pb2.FieldText,
    "f": resources_pb2.FieldFile,
    "u": resources_pb2.FieldLink,
    "k": resources_pb2.FieldKeyValue,
    # For conversations, kb_fields.value holds the FieldConversation metadata
    "c": resources_pb2.FieldConversation,
}

MIN_UUID = uuid.UUID(int=0)


@dataclass
class Stats:
    kbs: int = 0
    kbs_skipped: int = 0
    kv_keys: int = 0
    resources: int = 0
    resources_skipped: int = 0
    fields: int = 0
    conversation_pages: int = 0
    processed_fields: int = 0
    chunks: int = 0
    embeddings: int = 0
    failed_resources: list[tuple[str, str]] = field(default_factory=list)


@dataclass
class PGResource:
    rid: uuid.UUID
    slug: str | None
    basic: bytes | None
    origin: bytes | None
    security: bytes | None
    extra: bytes | None


@dataclass
class PGField:
    field_type: str
    field_id: str
    status: bytes | None
    value: bytes | None
    md5: str | None


@dataclass
class PGConversationPage:
    field_id: str
    page: int
    value: bytes | None


def _parse(pb_klass: type[Any], data: bytes | None) -> Any:
    if data is None:
        return None
    pb = pb_klass()
    pb.ParseFromString(bytes(data))
    return pb


# ---------------------------------------------------------------------------
# PostgreSQL readers
# ---------------------------------------------------------------------------


async def pg_iter_kbs(conn: psycopg.AsyncConnection, kbids: list[str] | None):
    async with conn.cursor() as cur:
        if kbids:
            await cur.execute(
                "SELECT kbid, slug, config, deleted_at FROM kbs "
                "WHERE kbid = ANY(%(kbids)s::uuid[]) ORDER BY kbid",
                {"kbids": kbids},
            )
        else:
            await cur.execute("SELECT kbid, slug, config, deleted_at FROM kbs ORDER BY kbid")
        rows = await cur.fetchall()
    for row in rows:
        yield row


async def pg_get_kb_kv(conn: psycopg.AsyncConnection, kbid: str) -> list[tuple[str, bytes]]:
    base = f"/kbs/{kbid}/"
    clauses = ["key = ANY(%(exact)s)"]
    params: dict[str, Any] = {"exact": [base + suffix for suffix in KB_KV_EXACT_KEYS]}
    for index, prefix in enumerate(KB_KV_PREFIX_KEYS):
        clauses.append(f"key LIKE %(prefix{index})s")
        params[f"prefix{index}"] = base + prefix + "%"
    async with conn.cursor() as cur:
        await cur.execute(
            f"SELECT key, value FROM resources WHERE {' OR '.join(clauses)} ORDER BY key",
            params,
        )
        return [(key, bytes(value)) for key, value in await cur.fetchall() if value is not None]


async def pg_get_worker_seqids(conn: psycopg.AsyncConnection) -> list[tuple[str, bytes]]:
    prefix = TXNID.format(worker="")
    async with conn.cursor() as cur:
        await cur.execute(
            "SELECT key, value FROM resources WHERE key LIKE %(prefix)s ORDER BY key",
            {"prefix": prefix + "%"},
        )
        return [(key, bytes(value)) for key, value in await cur.fetchall() if value is not None]


async def pg_get_resources_batch(
    conn: psycopg.AsyncConnection, kbid: str, after: uuid.UUID, limit: int,
    rid: uuid.UUID | None = None,
) -> list[PGResource]:
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT rid, slug, basic, origin, security, extra
            FROM kb_resources
                        WHERE kbid = %(kbid)s
                            AND ((%(rid)s::uuid IS NULL AND rid > %(after)s) OR rid = %(rid)s::uuid)
            ORDER BY rid
            LIMIT %(limit)s
            """,
            {"kbid": kbid, "after": after, "limit": limit, "rid": rid},
        )
        return [PGResource(*row) for row in await cur.fetchall()]


async def pg_get_fields(
    conn: psycopg.AsyncConnection, kbid: str, rids: list[uuid.UUID]
) -> dict[uuid.UUID, list[PGField]]:
    result: dict[uuid.UUID, list[PGField]] = defaultdict(list)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT rid, field_type, field_id, status, value, md5
            FROM kb_fields
            WHERE kbid = %(kbid)s AND rid = ANY(%(rids)s)
            """,
            {"kbid": kbid, "rids": rids},
        )
        async for rid, *values in cur:
            result[rid].append(PGField(*values))
    return result


async def pg_get_conversation_pages(
    conn: psycopg.AsyncConnection, kbid: str, rids: list[uuid.UUID]
) -> dict[uuid.UUID, list[PGConversationPage]]:
    result: dict[uuid.UUID, list[PGConversationPage]] = defaultdict(list)
    async with conn.cursor() as cur:
        await cur.execute(
            """
            SELECT rid, field_id, page, value
            FROM kb_conversations
            WHERE kbid = %(kbid)s AND rid = ANY(%(rids)s)
            """,
            {"kbid": kbid, "rids": rids},
        )
        async for rid, *values in cur:
            result[rid].append(PGConversationPage(*values))
    return result


# ---------------------------------------------------------------------------
# Conversion to MarkLogic documents
# ---------------------------------------------------------------------------


def build_resource_content(resource: PGResource, rid: str) -> ResourceCreateContent:
    basic = _parse(resources_pb2.Basic, resource.basic) or resources_pb2.Basic()
    origin = _parse(resources_pb2.Origin, resource.origin)
    extra = _parse(resources_pb2.Extra, resource.extra)
    security = _parse(resources_pb2.Security, resource.security)

    # Same rules as ProcessorMarkLogic.writer_resource_content
    slug = resource.slug or basic.slug or rid
    basic.slug = slug
    created_at = (
        origin.created.ToJsonString()
        if origin is not None and origin.HasField("created")
        else basic.created.ToJsonString()
        if basic.HasField("created")
        else ""
    )
    modified_at = (
        origin.modified.ToJsonString()
        if origin is not None and origin.HasField("modified")
        else basic.modified.ToJsonString()
        if basic.HasField("modified")
        else ""
    )
    return ResourceCreateContent(
        slug=slug,
        basic=documents.to_json(basic),
        origin=documents.to_json(origin) if origin is not None else None,
        extra=documents.to_json(extra) if extra is not None else None,
        security=documents.to_json(security) if security is not None else None,
        title=basic.title or slug,
        created_at=created_at,
        modified_at=modified_at,
        processing_status=writer_pb2.Metadata.Status.Name(basic.metadata.status),
        facets=sorted(
            {
                f"/l/{classification.labelset}/{classification.label}"
                for classification in basic.usermetadata.classifications
                if not classification.cancelled_by_user
            }
        ),
    )


def build_field_document(rid: str, pg_field: PGField) -> Document:
    value: dict[str, Any] | None = None
    if pg_field.value is not None:
        pb_klass = FIELD_VALUE_PB.get(pg_field.field_type)
        if pb_klass is not None:
            value = documents.to_json(_parse(pb_klass, pg_field.value))
        else:
            # Same fallback as datamanagers.fields.set for raw payloads
            value = {"_raw_bytes_base64": base64.b64encode(bytes(pg_field.value)).decode("ascii")}
    status = _parse(writer_pb2.FieldStatus, pg_field.status)
    content = FieldContent(
        resource_id=rid,
        field_id=pg_field.field_id,
        field_type=pg_field.field_type,
        value=value,
        md5=pg_field.md5,
        status=documents.to_json(status) if status is not None else None,
    )
    return Document(
        uri=MarkLogicURIs.field(rid, pg_field.field_type, pg_field.field_id),
        collections=[MarkLogicCollections.FIELDS],
        content=content.model_dump(mode="json"),
        content_type="application/json",
    )


def build_conversation_page_document(rid: str, page: PGConversationPage) -> Document | None:
    if page.value is None:
        return None
    # Page 0 is the SplitsMetadata sentinel, both in PostgreSQL and MarkLogic
    pb_klass = resources_pb2.SplitsMetadata if page.page == 0 else resources_pb2.Conversation
    content = ConversationPageContent(
        resource_id=rid,
        field_id=page.field_id,
        conversation_page=page.page,
        value=documents.to_json(_parse(pb_klass, page.value)),
    )
    return Document(
        uri=MarkLogicURIs.conversation_page(rid, page.field_id, page.page),
        collections=[MarkLogicCollections.CONVERSATIONS],
        content=content.model_dump(mode="json"),
        content_type="application/json",
    )


# ---------------------------------------------------------------------------
# MarkLogic writers
# ---------------------------------------------------------------------------


@backoff.on_exception(
    backoff.expo,
    MarkLogicResponseError,
    jitter=backoff.random_jitter,
    max_time=60,
    giveup=lambda exc: exc.code != "XDMP-DBTIMESTAMP",
)
async def migrate_kb_metadata(
    conn: psycopg.AsyncConnection,
    driver: MarkLogicDriver,
    stats: Stats,
    *,
    kbid: str,
    slug: str,
    config: knowledgebox_pb2.KnowledgeBoxConfig | None,
) -> None:
    async with driver.transaction(system=True, read_only=False) as txn:
        await datamanagers.kb.set_slug(txn, slug=slug, kbid=kbid)
        await txn.commit()

    await driver.ensure_kb_database(kbid)

    kv_items = await pg_get_kb_kv(conn, kbid)
    vectorsets_key = datamanagers.vectorsets.KB_VECTORSETS.format(kbid=kbid)
    vectorsets = knowledgebox_pb2.KnowledgeBoxVectorSetsConfig()
    for key, value in kv_items:
        if key == vectorsets_key:
            vectorsets.ParseFromString(value)
            break
    async with driver.transaction(kbid=kbid, read_only=False) as txn:
        await datamanagers.kb.set(txn, kbid=kbid, config=config)
        for key, value in kv_items:
            await txn.set(key, value)
        await txn.commit()
    for vectorset_config in vectorsets.vectorsets:
        dimension = vectorset_config.vectorset_index_config.vector_dimension
        if dimension <= 0:
            raise ValueError(f"KB {kbid}: vectorset {vectorset_config.vectorset_id} has no dimension")
        await vector_views.ensure_view(
            driver, driver.kb_database(kbid), vectorset_config.vectorset_id, dimension
        )
    stats.kv_keys += len(kv_items)


async def migrate_resource(
    driver: MarkLogicDriver,
    *,
    kbid: str,
    resource: PGResource,
    fields: list[PGField],
    pages: list[PGConversationPage],
    storage: Storage,
) -> tuple[int, int, int, int, int] | None:
    rid = resource.rid.hex
    processed_fields = chunks_count = embeddings_count = 0

    async with driver.transaction(kbid=kbid, read_only=False) as txn:
        if await datamanagers.resources.exists(txn, kbid=kbid, rid=rid):
            return None
        content = build_resource_content(resource, rid)
        docs = [build_field_document(rid, field) for field in fields]
        page_docs = [doc for doc in (build_conversation_page_document(rid, page) for page in pages) if doc]
        await datamanagers.resources.create_resource(txn, kbid=kbid, rid=rid, data=content)
        if docs or page_docs:
            await documents.write_many(txn, docs + page_docs)
        orm_resource = Resource(
            txn=txn, storage=storage, kbid=kbid, uuid=rid, disable_vectors=False
        )
        index_message = await IndexMessageBuilderV2(orm_resource).full(reindex=True)
        for field, chunks, embeddings in await get_field_processed_contents(txn, kbid, index_message):
            await datamanagers.fields.create_processed_documents(
                txn, field=field, chunks=chunks, embeddings=embeddings
            )
            processed_fields += 1
            chunks_count += len(chunks)
            embeddings_count += len(embeddings)
        await txn.commit()
    return len(docs), len(page_docs), processed_fields, chunks_count, embeddings_count


async def migrate_kb_resources(
    conn: psycopg.AsyncConnection,
    driver: MarkLogicDriver,
    stats: Stats,
    *,
    kbid: str,
    batch_size: int,
    concurrency: int,
    storage: Storage,
    rid: uuid.UUID | None = None,
) -> None:
    semaphore = asyncio.Semaphore(concurrency)

    async def _migrate(
        resource: PGResource, fields: list[PGField], pages: list[PGConversationPage]
    ) -> None:
        async with semaphore:
            try:
                result = await migrate_resource(
                    driver, kbid=kbid, resource=resource, fields=fields, pages=pages, storage=storage
                )
            except Exception as exc:
                logger.exception(
                    "Error migrating resource", extra={"kbid": kbid, "rid": resource.rid.hex}
                )
                stats.failed_resources.append((kbid, f"{resource.rid.hex}: {exc!r}"))
                return
            if result is None:
                stats.resources_skipped += 1
                return
            n_fields, n_pages, n_processed, n_chunks, n_embeddings = result
            stats.resources += 1
            stats.fields += n_fields
            stats.conversation_pages += n_pages
            stats.processed_fields += n_processed
            stats.chunks += n_chunks
            stats.embeddings += n_embeddings

    after = MIN_UUID
    migrated = 0
    while True:
        batch = await pg_get_resources_batch(conn, kbid, after, batch_size, rid=rid)
        if not batch:
            if rid is not None:
                raise ValueError(f"Resource {rid.hex} not found in PostgreSQL KB {kbid}")
            break
        rids = [r.rid for r in batch]
        fields_by_rid = await pg_get_fields(conn, kbid, rids)
        pages_by_rid = await pg_get_conversation_pages(conn, kbid, rids)
        await asyncio.gather(
            *(_migrate(r, fields_by_rid.get(r.rid, []), pages_by_rid.get(r.rid, [])) for r in batch)
        )
        migrated += len(batch)
        after = batch[-1].rid
        logger.info(f"KB {kbid}: {migrated} resources processed")
        if rid is not None:
            break


async def migrate_worker_seqids(
    conn: psycopg.AsyncConnection, driver: MarkLogicDriver, stats: Stats
) -> None:
    items = await pg_get_worker_seqids(conn)
    async with driver.transaction(system=True, read_only=False) as txn:
        for key, value in items:
            await txn.set(key, value)
        await txn.commit()
    stats.kv_keys += len(items)
    logger.info(f"Migrated {len(items)} ingest worker sequence ids")


async def run(args: argparse.Namespace) -> Stats:
    stats = Stats()
    driver = await setup_driver()
    if not isinstance(driver, MarkLogicDriver):
        raise RuntimeError("Target maindb driver must be MarkLogic")
    storage = await get_storage()
    try:
        async with await psycopg.AsyncConnection.connect(args.pg_url, autocommit=True) as conn:
            if not args.skip_kb_metadata:
                await migrate_worker_seqids(conn, driver, stats)
            async for kb_uuid, slug, config_bytes, deleted_at in pg_iter_kbs(
                conn, args.kbid
            ):
                kbid = str(kb_uuid)
                if slug is None or deleted_at is not None:
                    logger.info(f"Skipping deleted KB {kbid}")
                    stats.kbs_skipped += 1
                    continue

                logger.info(f"Migrating KB {kbid} ({slug})")
                if not args.skip_kb_metadata:
                    await migrate_kb_metadata(
                        conn,
                        driver,
                        stats,
                        kbid=kbid,
                        slug=slug,
                        config=_parse(knowledgebox_pb2.KnowledgeBoxConfig, config_bytes),
                    )
                if not args.skip_resources:
                    await migrate_kb_resources(
                        conn,
                        driver,
                        stats,
                        kbid=kbid,
                        batch_size=args.batch_size,
                        concurrency=args.concurrency,
                        storage=storage,
                        rid=args.rid,
                    )
                stats.kbs += 1
            if args.rid is not None and stats.kbs == 0:
                raise ValueError(f"No active PostgreSQL KB found for {args.kbid[0]}")
    finally:
        await teardown_storage()
        await teardown_driver()
    return stats


def parse_arguments(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migrate PostgreSQL metadata and rebuild MarkLogic indexes from object storage"
    )
    parser.add_argument(
        "--pg-url",
        default=os.environ.get("DRIVER_PG_URL"),
        help="Legacy PostgreSQL maindb URL (defaults to DRIVER_PG_URL env var)",
    )
    parser.add_argument(
        "--kbid",
        action="append",
        help="Only migrate the given KB. Can be repeated. Defaults to all KBs.",
    )
    parser.add_argument("--rid", type=uuid.UUID, help="Only migrate this resource; requires one --kbid")
    parser.add_argument("--batch-size", type=int, default=200, help="Resources read from PG per batch")
    parser.add_argument(
        "--concurrency", type=int, default=10, help="Resources written concurrently to MarkLogic"
    )
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument(
        "--skip-resources",
        action="store_true",
        help="Only migrate KB registry, config and KB maindb keys",
    )
    scope.add_argument(
        "--skip-kb-metadata",
        action="store_true",
        help="Only migrate resources; assumes KB databases, metadata and vector views already exist",
    )
    args = parser.parse_args(argv)
    if not args.pg_url:
        parser.error("--pg-url or DRIVER_PG_URL is required")
    if args.batch_size <= 0 or args.concurrency <= 0:
        parser.error("--batch-size and --concurrency must be positive")
    if args.rid is not None:
        if not args.kbid or len(args.kbid) != 1:
            parser.error("--rid requires exactly one --kbid")
        if args.skip_resources:
            parser.error("--rid cannot be combined with --skip-resources")
    return args


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_arguments(argv)
    stats = asyncio.run(run(args))
    logger.info(
        f"Done. kbs={stats.kbs} kbs_skipped={stats.kbs_skipped} kv_keys={stats.kv_keys} "
        f"resources={stats.resources} resources_skipped={stats.resources_skipped} fields={stats.fields} "
        f"conversation_pages={stats.conversation_pages} processed_fields={stats.processed_fields} "
        f"chunks={stats.chunks} embeddings={stats.embeddings} failed={len(stats.failed_resources)}"
    )
    for kbid, error in stats.failed_resources:
        logger.error(f"Failed resource in KB {kbid}: {error}")
    if stats.failed_resources:
        sys.exit(1)


if __name__ == "__main__":
    main()
