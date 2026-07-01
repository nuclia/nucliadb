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
  └── backfill_kb                  (shards, slug, config)
      └── backfill_resource        (basic, slug, shard, origin, extra,
      │                             security)
          └── backfill_field       (value, status, md5)
              └── backfill_conversation_field  (metadata + every page
                                               + splits_metadata)

Each level reads from v1 (raw KV) and writes to v2 (pg tables) inside its own
read-write transaction, so a failure in one resource/field does not roll back
the whole KB.
"""

import logging

from nucliadb.common import datamanagers, file_md5_v2, locking
from nucliadb.common.context import ApplicationContext
from nucliadb.common.datamanagers import (
    conversations as conversations_v1,
)
from nucliadb.common.datamanagers import (
    conversations_v2,
    fields_v2,
    kb_v2,
    resources_v2,
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
from nucliadb.common.datamanagers.utils import with_ro_transaction, with_rw_transaction
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.models_utils import from_proto
from nucliadb_protos import resources_pb2
from nucliadb_telemetry.logs import setup_logging
from nucliadb_telemetry.settings import LogLevel, LogSettings

logger = logging.getLogger("backfill_orm_tables")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


async def backfill_all_kbs(minimal: bool) -> None:
    """Iterate every KB in v1 and backfill it into the ORM tables.

    Args:
        minimal: When True, only the bare-minimum rows are written (slug, status).
            Use this mode to safely activate the write feature flag: it ensures every
            existing resource and field already has an entry in the ORM tables, so an
            in-flight write that arrives before the full backfill completes will not
            fail due to a missing row.  Heavy data (config, shards, origin, extra,
            security, conversation pages) can then be populated in a follow-up run
            with minimal=False.
    """
    kbids_and_slugs = []
    async with with_ro_transaction() as txn:
        async for kbid, slug in kb_v1.get_kbs(txn):
            kbids_and_slugs.append((kbid, slug))
    for kbid, _ in kbids_and_slugs:
        try:
            await backfill_kb(kbid=kbid, minimal=minimal)
        except Exception:
            logger.exception("Failed to backfill KB %s (%s), continuing", kbid, slug)


# ---------------------------------------------------------------------------
# KB
# ---------------------------------------------------------------------------


async def backfill_kb(*, kbid: str, minimal: bool) -> None:
    """Backfill one KB row and all of its resources.

    Args:
        minimal: See :func:`backfill_all_kbs` for a full description.  In short,
            set this to True when populating the ORM tables just before enabling the
            write feature flag, so that any concurrent write for an already-existing
            resource or field finds a row to update instead of failing with a
            missing-entry error.
    """
    logger.info(f"Backfilling KB {kbid}")
    start_time = asyncio.get_event_loop().time()
    async with with_rw_transaction() as txn:
        try:
            await backfill_kb_metadata(txn, kbid=kbid, minimal=minimal)
            await txn.commit()
        except Exception:
            logger.exception(f"Failed to backfill KB metadata for {kbid}, skipping")
            return

    # Iterate resources in their own transactions
    async for rid in datamanagers.resources.iterate_resource_ids(kbid=kbid):
        try:
            await backfill_resource(kbid=kbid, rid=rid, minimal=minimal)
        except Exception:
            logger.exception(f"Failed to backfill resource {kbid}/{rid}, continuing")
    elapsed = asyncio.get_event_loop().time() - start_time
    logger.info(f"Backfilled KB {kbid} in {elapsed:.2f} seconds")


async def backfill_kb_metadata(txn: Transaction, *, kbid: str, minimal: bool) -> None:
    config = await datamanagers.kb.get_config(txn, kbid=kbid, for_update=True)
    if config is None:
        raise ValueError(f"KB {kbid} has no config, skipping backfill of config and shards")

    # slug
    slug = config.slug
    await kb_v2.set_kbid_for_slug(txn, kbid=kbid, slug=slug)

    if minimal:
        logger.info(f"Minimal backfill for KB {kbid}, skipping config and shards")
        return

    # config
    await kb_v2.set_config(txn, kbid=kbid, config=config)

    # shards
    shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid, for_update=True)
    if shards is None:
        raise ValueError(f"KB {kbid} has no shards, skipping backfill of shards")

    await kb_v2.update_kb_shards(txn, kbid=kbid, shards=shards)


# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------


async def backfill_resource(*, kbid: str, rid: str, minimal: bool) -> None:
    """Backfill one kb_resources row and all of its fields."""
    logger.info(f"Backfilling resource {kbid}/{rid}")

    async with locking.distributed_lock(locking.RESOURCE_LOCK.format(kbid=kbid, resource_id=rid)):
        async with with_rw_transaction() as txn:
            await backfill_resource_metadata(txn, kbid=kbid, rid=rid, minimal=minimal)
            await txn.commit()

        async with with_ro_transaction() as txn:
            all_fields = await datamanagers.resources.get_all_field_ids(txn, kbid=kbid, rid=rid)
            for field in all_fields.fields if all_fields is not None else []:
                field_type_str = from_proto.field_type_name(field.field_type).abbreviation()
                try:
                    await backfill_field(
                        kbid=kbid,
                        rid=rid,
                        field_type=field_type_str,
                        field_id=field.field,
                        minimal=minimal,
                    )
                except Exception:
                    logger.exception(
                        f"Failed to backfill field {kbid}/{rid}/{field_type_str}/{field.field}, continuing"
                    )


async def backfill_resource_metadata(txn: Transaction, *, kbid: str, rid: str, minimal: bool) -> None:
    basic = await resources_v1.get_basic(txn, kbid=kbid, rid=rid)
    if basic is None:
        raise ValueError(f"Resource {kbid}/{rid} has no basic metadata, skipping backfill")

    # slug
    await resources_v2.set_slug(txn, kbid=kbid, rid=rid, slug=basic.slug)
    if minimal:
        logger.info(
            f"Minimal backfill for resource {kbid}/{rid}, skipping shard, origin, extra, and security"
        )
        return

    # basic
    await resources_v2.set_basic(txn, kbid=kbid, rid=rid, basic=basic)

    # shard
    shard = await resources_v1.get_resource_shard_id(txn, kbid=kbid, rid=rid)
    if shard is None:
        raise ValueError(f"Resource {kbid}/{rid} has no shard, skipping backfill")

    await resources_v2.set_resource_shard_id(txn, kbid=kbid, rid=rid, shard=shard)

    # origin
    origin = await resources_v1.get_origin(txn, kbid=kbid, rid=rid)
    if origin is not None:
        await resources_v2.set_origin(txn, kbid=kbid, rid=rid, origin=origin)

    # extra
    extra = await resources_v1.get_extra(txn, kbid=kbid, rid=rid)
    if extra is not None:
        await resources_v2.set_extra(txn, kbid=kbid, rid=rid, extra=extra)

    # security
    security = await resources_v1.get_security(txn, kbid=kbid, rid=rid)
    if security is not None:
        await resources_v2.set_security(txn, kbid=kbid, rid=rid, security=security)


# ---------------------------------------------------------------------------
# Field
# ---------------------------------------------------------------------------


async def backfill_field(*, kbid: str, rid: str, field_type: str, field_id: str, minimal: bool) -> None:
    """Backfill one kb_fields row (value, status, md5)."""
    logger.info(f"Backfilling field {kbid}/{rid}/{field_type}/{field_id}")

    async with with_rw_transaction() as txn:
        # status
        status = await fields_v1.get_status(
            txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id
        )
        if status is not None:
            await fields_v2.set_status(
                txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id, status=status
            )
            if minimal:
                logger.info(
                    f"Minimal backfill for field {kbid}/{rid}/{field_type}/{field_id}, skipping value and md5"
                )
                await txn.commit()
                return

        # value
        value = await fields_v1.get_raw(
            txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id
        )
        if value is not None:
            await fields_v2.set(
                txn, kbid=kbid, rid=rid, field_type=field_type, field_id=field_id, value=value
            )
            if minimal:
                logger.info(
                    f"Minimal backfill for field {kbid}/{rid}/{field_type}/{field_id}, skipping md5"
                )
                await txn.commit()
                return

        # md5 — only file fields (field_type == 'f') have an md5 in the v1 table
        if field_type == "f" and value is not None:
            field_file = resources_pb2.FieldFile()
            field_file.ParseFromString(value)
            if field_file.file.md5:
                await file_md5_v2.set(
                    txn, kbid=kbid, md5=field_file.file.md5, rid=rid, field_id=field_id
                )
                if minimal:
                    logger.info(
                        f"Minimal backfill for field {kbid}/{rid}/{field_type}/{field_id}, skipping md5"
                    )
                    await txn.commit()
                    return

        await txn.commit()

    # Conversation fields need their own dedicated backfill
    if field_type == "c":
        await backfill_conversation_field(kbid=kbid, rid=rid, field_id=field_id, minimal=minimal)


# ---------------------------------------------------------------------------
# Conversation field
# ---------------------------------------------------------------------------


async def backfill_conversation_field(*, kbid: str, rid: str, field_id: str, minimal: bool) -> None:
    """
    Backfill FieldConversation metadata, all pages, and SplitsMetadata

    Conversation metadata has already been backfilled in backfill_field in the kb_fields table.
    We need to backfill the conversation pages and splits metadata into the kb_conversations table.
    """
    logger.info(f"Backfilling conversation {kbid}/{rid}/c/{field_id}")

    async with with_rw_transaction() as txn:
        metadata = await conversations_v1.get_metadata(
            txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id
        )
        if metadata is None:
            raise ValueError(
                f"Conversation {kbid}/{rid}/c/{field_id} has no metadata, skipping backfill"
            )
        splits_metadata = await conversations_v1.get_splits_metadata(
            txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id
        )
        if splits_metadata is not None:
            await conversations_v2.set_splits_metadata(
                txn, kbid=kbid, rid=rid, field_id=field_id, splits_metadata=splits_metadata
            )
        await txn.commit()

        if minimal:
            logger.info(f"Minimal backfill for conversation {kbid}/{rid}/c/{field_id}, skipping pages")
            return

    async with with_rw_transaction() as txn:
        for page_n in range(1, metadata.pages + 1):
            page = await conversations_v1.get_page(
                txn, kbid=kbid, rid=rid, field_type="c", field_id=field_id, page=page_n
            )
            if page is None:
                logger.warning(
                    f"Conversation {kbid}/{rid}/c/{field_id} page {page_n} is missing, skipping"
                )
                continue
            await conversations_v2.set_page(
                txn, kbid=kbid, rid=rid, field_id=field_id, page=page_n, value=page
            )
            await txn.commit()


async def _main():
    import argparse

    parser = argparse.ArgumentParser(description="Backfill ORM tables from the v1 KV store")
    parser.add_argument(
        "--kbid", help="Backfill a single KB by its UUID (default: all KBs)", required=True
    )
    parser.add_argument(
        "--minimal",
        action=argparse.BooleanOptionalAction,
        default=True,
        help=(
            "When set, write only the bare-minimum rows (slug, status) needed to activate the "
            "write feature flag safely. Any in-flight write for an already-existing resource or "
            "field will find an ORM entry to update instead of failing. "
            "Heavy data (config, shards, origin, extra, security, conversation pages) is skipped "
            "and must be backfilled in a follow-up run with --no-minimal. Default: True"
        ),
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
        await backfill_kb(kbid=args.kbid, minimal=args.minimal)
    finally:
        await context.finalize()


if __name__ == "__main__":
    import asyncio

    asyncio.run(_main())
