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
Backfill: copy data from the v1 key-value store into the new ORM tables
(kbs, kb_resources, kb_fields, kb_conversations) created by migration 0016.

Hierarchy
---------
  backfill_all_kbs
  └── backfill_kb                  (slug, config, shards)
      └── backfill_resource        (slug, shard, basic, origin, extra, security,
                                    all fields, all conversation pages)
          └── [reconciliation]     compare v1 vs v2 resource listings and
                                   backfill any resource added during migration

Each KB's metadata is written in its own transaction.  Each resource (and all of
its fields and conversation pages) is migrated in a single transaction under a
distributed lock.  After all resources are processed, a reconciliation pass
catches any resource created concurrently between the initial v1 snapshot and the
end of the migration run.
"""

import asyncio
import logging
import time

from nucliadb.common import datamanagers, file_md5, locking
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers import (
    conversations as conversations_v1,
)
from nucliadb.common.datamanagers import (
    fields as fields_v1,
)
from nucliadb.common.datamanagers import (
    kb as kb_v1,
)
from nucliadb.common.datamanagers import (
    resources as resources_v1,
)
from nucliadb.common.datamanagers import (
    resources_v2,
)
from nucliadb.common.datamanagers.utils import _pg_cursor, with_ro_transaction, with_rw_transaction
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.models_utils import from_proto
from nucliadb_protos import resources_pb2, writer_pb2
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import LogLevel, LogSettings

logger = logging.getLogger("backfill_orm_tables")

# Maximum number of resources migrated concurrently within a single KB backfill.
# Each slot holds one distributed lock + one PG transaction, so keep this
# conservative enough not to saturate the connection pool.
_MAX_CONCURRENT_RESOURCES = 10

# Maximum number of reconciliation iterations to perform for each KB.
_MAX_RECONCILIATION_ITERATIONS = 2

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def backfill_all_kbs() -> None:
    """Iterate every KB in v1 and backfill it into the ORM tables."""
    kbids_and_slugs = []
    async with with_ro_transaction() as txn:
        async for kbid, slug in kb_v1.get_kbs(txn):
            kbids_and_slugs.append((kbid, slug))
    for kbid, slug in kbids_and_slugs:
        try:
            await backfill_kb(kbid=kbid)
        except Exception:
            logger.exception("Failed to backfill KB %s (%s), continuing", kbid, slug)


# ---------------------------------------------------------------------------
# KB
# ---------------------------------------------------------------------------


async def backfill_kb(*, kbid: str) -> None:
    """Backfill one KB row and all of its resources.

    After migrating all resources, a reconciliation pass compares the v1 and v2
    resource listings to catch any resources created concurrently during the
    migration run.
    """
    logger.info(f"Backfilling KB {kbid}")
    start_time = time.monotonic()

    async with with_rw_transaction() as txn:
        try:
            await _backfill_kb_metadata(txn, kbid=kbid)
            await txn.commit()
        except Exception:
            logger.exception(f"Failed to backfill KB metadata for {kbid}, skipping")
            return

    # Snapshot v1 resource IDs before starting the migration
    v1_rids: set[str] = set()
    async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
        v1_rids.add(rid)

    await _backfill_resources(kbid=kbid, rids=v1_rids)

    iteration = 0
    while True:
        if iteration >= _MAX_RECONCILIATION_ITERATIONS:
            logger.warning(
                f"Reconciliation: reached max iterations ({_MAX_RECONCILIATION_ITERATIONS}) for KB {kbid}, stopping"
            )
            break
        iteration += 1

        # Reconciliation: find resources present in v1 but absent from v2.
        # These are resources that were created after our initial v1 snapshot was
        # taken and would have been missed by the main loop above.
        v2_rids: set[str] = set()
        async for rid in resources_v2.iterate_resource_ids(kbid=kbid):
            v2_rids.add(rid)

        v1_rids_now: set[str] = set()
        async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
            v1_rids_now.add(rid)

        missed = v1_rids_now - v2_rids
        if missed:
            logger.info(
                f"Reconciliation: {len(missed)} resource(s) missing from v2 for KB {kbid}, backfilling"
            )
            await _backfill_resources(kbid=kbid, rids=missed)
        else:
            break

    elapsed = time.monotonic() - start_time
    logger.info(f"Backfilled KB {kbid} in {elapsed:.2f} seconds")


async def _backfill_kb_metadata(txn: Transaction, *, kbid: str) -> None:
    """Read all KB metadata from v1 and write it to the kbs table in a single INSERT."""
    config = await datamanagers.kb.get_config(txn, kbid=kbid, for_update=True)
    if config is None:
        raise ValueError(f"KB {kbid} has no config, skipping backfill")

    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
    if shards is None:
        raise ValueError(f"KB {kbid} has no shards, skipping backfill")

    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kbs (kbid, slug, config, shards)
            VALUES (%(kbid)s, %(slug)s, %(config)s, %(shards)s)
            ON CONFLICT (kbid) DO UPDATE SET
                slug   = EXCLUDED.slug,
                config = EXCLUDED.config,
                shards = EXCLUDED.shards
            """,
            {
                "kbid": kbid,
                "slug": config.slug,
                "config": config.SerializeToString(),
                "shards": shards.SerializeToString(),
            },
        )


async def _backfill_resources(*, kbid: str, rids: set[str]) -> None:
    """Backfill a set of resources concurrently, bounded by _MAX_CONCURRENT_RESOURCES."""
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_RESOURCES)

    async def _guarded(rid: str) -> None:
        async with semaphore:
            try:
                await backfill_resource(kbid=kbid, rid=rid)
            except Exception:
                logger.exception(f"Failed to backfill resource {kbid}/{rid}, continuing")

    await asyncio.gather(*(_guarded(rid) for rid in rids))


# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------


async def backfill_resource(*, kbid: str, rid: str) -> None:
    """Backfill one kb_resources row and all of its fields in a single transaction."""
    logger.info(f"Backfilling resource {kbid}/{rid}")
    async with locking.distributed_lock(locking.RESOURCE_LOCK.format(kbid=kbid, resource_id=rid)):
        async with with_rw_transaction() as txn:
            await _backfill_resource_in_txn(txn, kbid=kbid, rid=rid)
            await txn.commit()


async def _backfill_resource_in_txn(txn: Transaction, *, kbid: str, rid: str) -> None:
    """
    Read all data for a resource from v1 (metadata, fields, conversation pages)
    and write everything to the ORM tables in one shot:
      - one INSERT for the kb_resources row
      - one INSERT for each kb_fields row (including the FieldConversation metadata for conversation fields)
      - one INSERT for each kb_conversations rows (pages + splits sentinel)
    """
    # --- Resource row ---
    basic = await resources_v1.get_basic(txn, kbid=kbid, rid=rid)
    if basic is None:
        raise ValueError(f"Resource {kbid}/{rid} has no basic metadata, skipping backfill")

    shard = await resources_v1.get_resource_shard_id(txn, kbid=kbid, rid=rid)
    if shard is None:
        raise ValueError(f"Resource {kbid}/{rid} has no shard, skipping backfill")

    origin = await resources_v1.get_origin(txn, kbid=kbid, rid=rid)
    extra = await resources_v1.get_extra(txn, kbid=kbid, rid=rid)
    security = await resources_v1.get_security(txn, kbid=kbid, rid=rid)

    async with _pg_cursor(txn) as cur:
        await cur.execute(
            """
            INSERT INTO kb_resources (kbid, rid, slug, shard, basic, origin, extra, security)
            VALUES (%(kbid)s, %(rid)s, %(slug)s, %(shard)s, %(basic)s, %(origin)s, %(extra)s, %(security)s)
            ON CONFLICT (kbid, rid) DO UPDATE SET
                slug     = EXCLUDED.slug,
                shard    = EXCLUDED.shard,
                basic    = EXCLUDED.basic,
                origin   = EXCLUDED.origin,
                extra    = EXCLUDED.extra,
                security = EXCLUDED.security
            """,
            {
                "kbid": kbid,
                "rid": rid,
                "slug": basic.slug,
                "shard": shard,
                "basic": basic.SerializeToString(),
                "origin": origin.SerializeToString() if origin is not None else None,
                "extra": extra.SerializeToString() if extra is not None else None,
                "security": security.SerializeToString() if security is not None else None,
            },
        )

    # --- Collect all field and conversation rows ---
    all_fields = await datamanagers.resources.get_all_field_ids(txn, kbid=kbid, rid=rid)

    # Add title and summary in the fields table, even though they are stored in the kb_resources.basic column.
    # We need to do this to have the status API work correctly for title and summary fields.
    title_field = resources_pb2.FieldID(field_type=writer_pb2.FieldType.GENERIC, field="title")
    summary_field = resources_pb2.FieldID(field_type=writer_pb2.FieldType.GENERIC, field="summary")
    if basic.title and title_field not in all_fields.fields:
        all_fields.fields.append(title_field)
    if basic.summary and summary_field not in all_fields.fields:
        all_fields.fields.append(summary_field)

    for field in all_fields.fields:
        field_type_str = from_proto.field_type_name(field.field_type).abbreviation()
        field_id = field.field

        status = await fields_v1.get_status(
            txn, kbid=kbid, rid=rid, field_type=field_type_str, field_id=field_id
        )
        value = await fields_v1.get_raw(
            txn, kbid=kbid, rid=rid, field_type=field_type_str, field_id=field_id
        )

        md5 = None
        if field_type_str == "f":
            md5 = await file_md5.get(txn, kbid=kbid, rid=rid, field_id=field_id)

        if field_type_str == "t" and value is not None:
            field_text = resources_pb2.FieldText()
            field_text.ParseFromString(value)
            md5 = field_text.md5 or None

        async with _pg_cursor(txn) as cur:
            await cur.execute(
                """
                INSERT INTO kb_fields (kbid, rid, field_type, field_id, value, md5, status)
                VALUES (%(kbid)s, %(rid)s, %(field_type)s, %(field_id)s, %(value)s, %(md5)s, %(status)s)
                ON CONFLICT (kbid, rid, field_type, field_id) DO UPDATE SET
                    value  = EXCLUDED.value,
                    md5     = EXCLUDED.md5,
                    status = EXCLUDED.status
                """,
                {
                    "kbid": kbid,
                    "rid": rid,
                    "field_type": field_type_str,
                    "field_id": field_id,
                    "value": value,
                    "md5": md5,
                    "status": status.SerializeToString() if status is not None else None,
                },
            )

        # Conversation fields: insert splits metadata sentinel + each page individually
        if field_type_str == "c" and value is not None:
            # Parse page count directly from the already-fetched field value
            # (FieldConversation is stored at the same KV key as the field value)
            conv_metadata = resources_pb2.FieldConversation()
            conv_metadata.ParseFromString(value)

            splits_metadata = await conversations_v1.get_splits_metadata(
                txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id
            )
            if splits_metadata is not None:
                async with _pg_cursor(txn) as cur:
                    await cur.execute(
                        """
                        INSERT INTO kb_conversations (kbid, rid, field_type, field_id, page, value)
                        VALUES (%(kbid)s, %(rid)s, 'c', %(field_id)s, 0, %(value)s)
                        ON CONFLICT (kbid, rid, field_type, field_id, page) DO UPDATE SET
                            value = EXCLUDED.value
                        """,
                        {
                            "kbid": kbid,
                            "rid": rid,
                            "field_id": field_id,
                            "value": splits_metadata.SerializeToString(),
                        },
                    )

            for page_n in range(1, conv_metadata.pages + 1):
                page = await conversations_v1.get_page(
                    txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id, page=page_n
                )
                if page is None:
                    logger.warning(
                        f"Conversation {kbid}/{rid}/c/{field_id} page {page_n} missing, skipping"
                    )
                    continue
                async with _pg_cursor(txn) as cur:
                    await cur.execute(
                        """
                        INSERT INTO kb_conversations (kbid, rid, field_type, field_id, page, value)
                        VALUES (%(kbid)s, %(rid)s, 'c', %(field_id)s, %(page)s, %(value)s)
                        ON CONFLICT (kbid, rid, field_type, field_id, page) DO UPDATE SET
                            value = EXCLUDED.value
                        """,
                        {
                            "kbid": kbid,
                            "rid": rid,
                            "field_id": field_id,
                            "page": page_n,
                            "value": page.SerializeToString(),
                        },
                    )


async def _main():
    import argparse

    parser = argparse.ArgumentParser(description="Backfill ORM tables from the v1 KV store")
    parser.add_argument(
        "--kbid",
        help="KB UUID to backfill, or the special value 'ALL_KBS' to backfill every KB.",
        required=True,
    )
    args = parser.parse_args()
    setup_logging(
        settings=LogSettings(
            debug=True,
            log_level=LogLevel.INFO,
            logger_levels={
                "backfill_orm_tables": LogLevel.INFO,
            },
        )
    )
    context = ApplicationContext(
        kv_driver=True,
        blob_storage=False,
        shard_manager=False,
        partitioning=False,
        nats_manager=False,
        transaction=False,
        nidx=False,
    )
    await context.initialize()

    try:
        if args.kbid == "ALL_KBS":
            await backfill_all_kbs()
        else:
            await backfill_kb(kbid=args.kbid)
    finally:
        await context.finalize()


if __name__ == "__main__":
    asyncio.run(_main())
