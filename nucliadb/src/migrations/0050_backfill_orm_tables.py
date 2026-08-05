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

"""Migration #50

Backfills the KB to the new orm tables only if the KB has not been backfilled yet.

Backfill: copy data from the v1 key-value store into the new ORM tables
(kbs, kb_resources, kb_fields, kb_conversations).

Hierarchy
---------
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
import uuid
from collections.abc import AsyncGenerator, AsyncIterator
from typing import cast

import backoff

from nucliadb.common import datamanagers, locking
from nucliadb.common.datamanagers import (
    kb as kbs_v2,
)
from nucliadb.common.datamanagers import (
    resources as resources_v2,
)
from nucliadb.common.datamanagers.utils import (
    _pg_cursor,
    get_kv_pb,
    with_ro_transaction,
    with_rw_transaction,
)
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.pg import PGTransaction
from nucliadb.common.models_utils import from_proto
from nucliadb.ingest.settings import settings as ingest_settings
from nucliadb.migrator.context import ExecutionContext
from nucliadb_protos import knowledgebox_pb2, resources_pb2, writer_pb2

#
from nucliadb_protos.resources_pb2 import Conversation as PBConversation
from nucliadb_protos.resources_pb2 import FieldConversation, SplitsMetadata

logger = logging.getLogger(__name__)


# Maximum number of resources migrated concurrently within a single KB backfill.
# Each slot holds one distributed lock + one PG transaction, so keep this
# conservative enough not to saturate the connection pool.
_MAX_CONCURRENT_RESOURCES = 20

# Maximum number of resource tasks to create at once.
_RESOURCE_TASK_BATCH_SIZE = 1000

# Maximum number of reconciliation iterations to perform for each KB.
_MAX_RECONCILIATION_ITERATIONS = 2


async def migrate(context: ExecutionContext) -> None: ...


async def migrate_kb(context: ExecutionContext, kbid: str) -> None:

    if not await should_backfill_kb(kbid):
        logger.info("KB does not need to be backfilled", extra={"kbid": kbid})
        return

    await backfill_kb(kbid=kbid)


async def should_backfill_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        if not await kbs_v2.exists(txn, kbid=kbid):
            logger.warning(
                "KB should be backfilled, as it does not exist in the new orm tables",
                extra={"kbid": kbid},
            )
            return True

        all_resources_v1: set[str] = set()
        async for rid in resources_v1.iterate_ids(kbid=kbid):
            if "-" in rid:
                logger.warning(f"Resource {kbid}/{rid} has a non-hex ID")
                all_resources_v1.add(uuid.UUID(rid).hex)
            else:
                all_resources_v1.add(rid)

        all_resources_v2 = {rid async for rid in resources_v2.iterate_ids(kbid=kbid)}
        if all_resources_v1 != all_resources_v2:
            missing_v1 = all_resources_v2 - all_resources_v1
            missing_v2 = all_resources_v1 - all_resources_v2
            logger.warning(
                "KB should be backfilled, as the set of resources in the new orm tables does not match the old ones",
                extra={
                    "kbid": kbid,
                    "missing_v1": list(missing_v1)[:10],  # limit to first 10 for logging
                    "missing_v2": list(missing_v2)[:10],  # limit to first 10 for logging
                },
            )
            return True
    return False


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
    async for rid in resources_v1.iterate_ids(kbid=kbid):
        v1_rids.add(rid)

    await _backfill_resources(
        kbid=kbid,
        rids=v1_rids,
    )

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
        async for rid in resources_v2.iterate_ids(kbid=kbid):
            v2_rids.add(rid)

        v1_rids_now: set[str] = set()
        async for rid in resources_v1.iterate_ids(kbid=kbid):
            if "-" in rid:
                logger.warning(f"Resource {kbid}/{rid} has a non-hex ID")
                v1_rids_now.add(uuid.UUID(rid).hex)
            else:
                v1_rids_now.add(rid)

        missed = v1_rids_now - v2_rids
        if missed:
            logger.warning(
                f"Reconciliation: {len(missed)} resource(s) missing from v2 for KB {kbid}, backfilling",
                extra={
                    "missed_resource_ids": list(missed),
                },
            )
            await _backfill_resources(
                kbid=kbid,
                rids=missed,
            )
        else:
            break

    elapsed = time.monotonic() - start_time
    logger.info(f"Backfilled KB {kbid} in {elapsed:.2f} seconds")


async def _backfill_kb_metadata(txn: Transaction, *, kbid: str) -> None:
    """Read all KB metadata from v1 and write it to the kbs table in a single INSERT."""
    config = await kbs_v1.get_config(txn, kbid=kbid, for_update=True)
    if config is None:
        raise ValueError(f"KB {kbid} has no config, skipping backfill")

    shards = await cluster_v1.get_kb_shards(txn, kbid=kbid, for_update=True)
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


async def _backfill_resources(
    *,
    kbid: str,
    rids: set[str],
) -> None:
    """Backfill a set of resources concurrently, bounded by _MAX_CONCURRENT_RESOURCES."""
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT_RESOURCES)

    pending = list(rids)
    logger.info(f"Backfilling {len(pending)} resource(s) for KB {kbid}")

    async def _guarded(rid: str, index: int) -> str | None:
        async with semaphore:
            try:
                await backfill_resource(kbid=kbid, rid=rid, index=index)
                return rid
            except Exception:
                logger.exception(f"Failed to backfill resource {kbid}/{rid}, continuing")
                return None

    for batch_start in range(0, len(pending), _RESOURCE_TASK_BATCH_SIZE):
        batch = pending[batch_start : batch_start + _RESOURCE_TASK_BATCH_SIZE]
        await asyncio.gather(*(_guarded(rid, batch_start + index) for index, rid in enumerate(batch)))


# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------


async def backfill_resource(*, kbid: str, rid: str, index: int) -> None:
    """Backfill one kb_resources row and all of its fields in a single transaction."""
    if index % 50_000 == 0 and index > 0:
        logger.info(f"Backfilling resource {kbid}/{rid} ({index})")
    else:
        logger.debug(f"Backfilling resource {kbid}/{rid}")
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

    shard = await resources_v1.get_shard_id(txn, kbid=kbid, rid=rid)
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
    all_fields = await resources_v1.get_all_field_ids(txn, kbid=kbid, rid=rid)
    if all_fields is None:
        return

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


class cluster_v1:
    KB_SHARDS = "/kbs/{kbid}/shards"

    @classmethod
    async def get_kb_shards(
        cls, txn: Transaction, *, kbid: str, for_update: bool = False
    ) -> writer_pb2.Shards | None:
        key = cls.KB_SHARDS.format(kbid=kbid)
        return await get_kv_pb(txn, key, writer_pb2.Shards, for_update=for_update)


class kbs_v1:
    KB_UUID = "/kbs/{kbid}/config"
    KB_SLUGS_BASE = "/kbslugs/"
    KB_SLUGS = KB_SLUGS_BASE + "{slug}"

    @classmethod
    async def get_kbs(cls, txn: Transaction, *, prefix: str = "") -> AsyncIterator[tuple[str, str]]:
        async for key in txn.keys(cls.KB_SLUGS.format(slug=prefix)):
            slug = key.replace(cls.KB_SLUGS_BASE, "")
            uuid = await cls.get_kbid(txn, slug=slug)
            if uuid is None:
                logger.error(f"KB with slug ({slug}) but without uuid?")
                continue
            yield (uuid, slug)

    @classmethod
    async def get_kbid(cls, txn: Transaction, *, slug: str) -> str | None:
        uuid = await txn.get(cls.KB_SLUGS.format(slug=slug), for_update=False)
        if uuid is not None:
            return uuid.decode()
        else:
            return None

    @classmethod
    async def get_config(
        cls, txn: Transaction, *, kbid: str, for_update: bool = False
    ) -> knowledgebox_pb2.KnowledgeBoxConfig | None:
        key = cls.KB_UUID.format(kbid=kbid)
        payload = await txn.get(key, for_update=for_update)
        if payload is None:
            return None
        response = knowledgebox_pb2.KnowledgeBoxConfig()
        response.ParseFromString(payload)
        return response


class resources_v1:
    KB_RESOURCE_BASIC = "/kbs/{kbid}/r/{uuid}"
    KB_RESOURCE_BASIC_FS = "/kbs/{kbid}/r/{uuid}/basic"  # Only used on FS driver
    KB_RESOURCE_ORIGIN = "/kbs/{kbid}/r/{uuid}/origin"
    KB_RESOURCE_EXTRA = "/kbs/{kbid}/r/{uuid}/extra"
    KB_RESOURCE_SECURITY = "/kbs/{kbid}/r/{uuid}/security"

    KB_RESOURCE_SLUG_BASE = "/kbs/{kbid}/s/"
    KB_RESOURCE_SLUG = f"{KB_RESOURCE_SLUG_BASE}{{slug}}"

    KB_RESOURCE_FIELDS = "/kbs/{kbid}/r/{uuid}/f/"

    KB_RESOURCE_ALL_FIELDS = "/kbs/{kbid}/r/{uuid}/allfields"
    KB_MATERIALIZED_RESOURCES_COUNT = "/kbs/{kbid}/materialized/resources/count"

    KB_RESOURCE_SHARD = "/kbs/{kbid}/r/{uuid}/shard"

    @classmethod
    @backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
    async def get_shard_id(
        cls, txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
    ) -> str | None:
        key = cls.KB_RESOURCE_SHARD.format(kbid=kbid, uuid=rid)
        shard = await txn.get(key, for_update=for_update)
        if shard is not None:
            return shard.decode()
        else:
            return None

    @classmethod
    async def get_basic(cls, txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Basic | None:
        raw = await cls.get_basic_raw(txn, kbid=kbid, rid=rid)
        if raw is None:
            return None
        basic = resources_pb2.Basic()
        basic.ParseFromString(raw)
        return basic

    @classmethod
    async def get_basic_raw(cls, txn: Transaction, *, kbid: str, rid: str) -> bytes | None:
        if ingest_settings.driver == "local":
            raw_basic = await txn.get(cls.KB_RESOURCE_BASIC_FS.format(kbid=kbid, uuid=rid))
        else:
            raw_basic = await txn.get(cls.KB_RESOURCE_BASIC.format(kbid=kbid, uuid=rid))
        return raw_basic

    @classmethod
    async def get_origin(cls, txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Origin | None:
        key = cls.KB_RESOURCE_ORIGIN.format(kbid=kbid, uuid=rid)
        return await get_kv_pb(txn, key, resources_pb2.Origin, for_update=False)

    @classmethod
    async def get_extra(cls, txn: Transaction, *, kbid: str, rid: str) -> resources_pb2.Extra | None:
        key = cls.KB_RESOURCE_EXTRA.format(kbid=kbid, uuid=rid)
        return await get_kv_pb(txn, key, resources_pb2.Extra, for_update=False)

    @classmethod
    async def get_security(
        cls, txn: Transaction, *, kbid: str, rid: str
    ) -> resources_pb2.Security | None:
        key = cls.KB_RESOURCE_SECURITY.format(kbid=kbid, uuid=rid)
        return await get_kv_pb(txn, key, resources_pb2.Security, for_update=False)

    @classmethod
    async def iterate_ids(cls, *, kbid: str) -> AsyncGenerator[str, None]:
        """
        Currently, the implementation of this is optimizing for reducing
        how long a transaction will be open since the caller controls
        how long each item that is yielded will be processed.

        For this reason, it is not using the `txn` argument passed in.
        """
        batch = []
        async for slug in cls._iter_resource_slugs(kbid=kbid):
            batch.append(slug)
            if len(batch) >= 200:
                for rid in await cls._get_resource_ids_from_slugs(kbid=kbid, slugs=batch):
                    yield rid
                batch = []
        if len(batch) > 0:
            for rid in await cls._get_resource_ids_from_slugs(kbid=kbid, slugs=batch):
                yield rid

    @classmethod
    @backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
    async def _iter_resource_slugs(cls, *, kbid: str) -> AsyncGenerator[str, None]:
        async with with_ro_transaction() as txn:
            async for key in txn.keys(match=cls.KB_RESOURCE_SLUG_BASE.format(kbid=kbid)):
                yield key.split("/")[-1]

    @classmethod
    @backoff.on_exception(backoff.expo, (Exception,), jitter=backoff.random_jitter, max_tries=3)
    async def _get_resource_ids_from_slugs(cls, kbid: str, slugs: list[str]) -> list[str]:
        async with with_ro_transaction() as txn:
            rids = await txn.batch_get(
                [cls.KB_RESOURCE_SLUG.format(kbid=kbid, slug=slug) for slug in slugs]
            )
        return [rid.decode() for rid in rids if rid is not None]

    @classmethod
    async def get_all_field_ids(
        cls, txn: Transaction, *, kbid: str, rid: str, for_update: bool = False
    ) -> resources_pb2.AllFieldIDs | None:
        key = cls.KB_RESOURCE_ALL_FIELDS.format(kbid=kbid, uuid=rid)
        return await get_kv_pb(txn, key, resources_pb2.AllFieldIDs, for_update=for_update)


class fields_v1:
    KB_RESOURCE_FIELD = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"
    KB_RESOURCE_FIELD_STATUS = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/status"

    @classmethod
    async def get_raw(
        cls, txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
    ) -> bytes | None:
        key = cls.KB_RESOURCE_FIELD.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
        return await txn.get(key)

    @classmethod
    async def get_status(
        cls, txn: Transaction, *, kbid: str, rid: str, field_type: str, field_id: str
    ) -> writer_pb2.FieldStatus | None:
        key = cls.KB_RESOURCE_FIELD_STATUS.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
        return await get_kv_pb(txn, key, writer_pb2.FieldStatus, for_update=False)


class conversations_v1:
    KB_CONVERSATION_PAGE = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/{page}"
    KB_CONVERSATION_SPLITS_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}/splits_metadata"
    KB_CONVERSATION_METADATA = "/kbs/{kbid}/r/{uuid}/f/{type}/{field}"

    @classmethod
    async def get_page(
        cls,
        txn: Transaction,
        *,
        kbid: str,
        rid: str,
        field_type: str,
        field_id: str,
        page: int,
    ) -> PBConversation | None:
        if page <= 0:
            raise ValueError("Conversation pages start at index 1")
        key = cls.KB_CONVERSATION_PAGE.format(
            kbid=kbid, uuid=rid, type=field_type, field=field_id, page=page
        )
        payload = await txn.get(key)
        if payload is None:
            return None
        pb = PBConversation()
        pb.ParseFromString(payload)
        return pb

    @classmethod
    async def get_metadata(
        cls,
        txn: Transaction,
        *,
        kbid: str,
        rid: str,
        field_type: str,
        field_id: str,
    ) -> FieldConversation | None:
        key = cls.KB_CONVERSATION_METADATA.format(kbid=kbid, uuid=rid, type=field_type, field=field_id)
        payload = await txn.get(key)
        if payload is None:
            return None
        pb = FieldConversation()
        pb.ParseFromString(payload)
        return pb

    @classmethod
    async def get_splits_metadata(
        cls,
        txn: Transaction,
        *,
        kbid: str,
        rid: str,
        field_type: str,
        field_id: str,
    ) -> SplitsMetadata | None:
        key = cls.KB_CONVERSATION_SPLITS_METADATA.format(
            kbid=kbid, uuid=rid, type=field_type, field=field_id
        )
        payload = await txn.get(key)
        if payload is None:
            return None
        pb = SplitsMetadata()
        pb.ParseFromString(payload)
        return pb


class file_md5:
    @classmethod
    async def get(cls, txn: Transaction, *, kbid: str, rid: str, field_id: str) -> str | None:
        """Get the MD5 hash for a resource field, or None if not found."""
        async with _pg_transaction(txn).connection.cursor() as cur:
            await cur.execute(
                "SELECT md5 FROM file_md5 WHERE kbid = %(kbid)s AND rid = %(rid)s AND field_id = %(field_id)s",
                {"kbid": kbid, "rid": rid, "field_id": f"f/{field_id}"},
            )
            row = await cur.fetchone()
            if row is None:
                return None
            return row[0]


def _pg_transaction(txn: Transaction) -> PGTransaction:
    return cast(PGTransaction, txn)
