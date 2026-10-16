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
"""Rewrite the processed documents (field texts, chunks and embeddings) of every resource of a KB.

Resource behaviours (basic, extra, security, origin) and field values are left untouched.
Progress is checkpointed in maindb after every batch so an interrupted rollover resumes where it left off.
"""

import asyncio
import logging
from datetime import datetime, timezone

from pydantic import BaseModel

from nucliadb.common import datamanagers, locking
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.processor.processor_marklogic import (
    IndexMessageBuilderV2,
    get_field_processed_contents,
)
from nucliadb.ingest.orm.resource import Resource
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

logger = logging.getLogger(__name__)

ROLLOVER_PROGRESS_KEY = "/kbs/{kbid}/rollover/progress"


class RolloverProgress(BaseModel):
    started_at: datetime
    last_resource_id: str | None = None
    processed: int = 0
    failed: int = 0


async def get_progress(driver: MarkLogicDriver, kbid: str) -> RolloverProgress | None:
    async with driver.ro_transaction(kbid=kbid) as txn:
        data = await txn.get(ROLLOVER_PROGRESS_KEY.format(kbid=kbid))
    return RolloverProgress.model_validate_json(data) if data else None


async def _set_progress(driver: MarkLogicDriver, kbid: str, progress: RolloverProgress) -> None:
    async with driver.rw_transaction(kbid=kbid) as txn:
        await txn.set(ROLLOVER_PROGRESS_KEY.format(kbid=kbid), progress.model_dump_json().encode())
        await txn.commit()


async def _delete_progress(driver: MarkLogicDriver, kbid: str) -> None:
    async with driver.rw_transaction(kbid=kbid) as txn:
        await txn.delete(ROLLOVER_PROGRESS_KEY.format(kbid=kbid))
        await txn.commit()


async def rollover_resource(driver: MarkLogicDriver, storage: Storage, kbid: str, rid: str) -> bool:
    """Rewrite the processed documents of a resource. Returns False if the resource no longer exists."""
    async with (
        locking.distributed_lock(locking.RESOURCE_LOCK.format(kbid=kbid, resource_id=rid)),
        driver.rw_transaction(kbid=kbid) as txn,
    ):
        if not await datamanagers.resources.exists(txn, kbid=kbid, rid=rid):
            return False
        resource = Resource(txn=txn, storage=storage, kbid=kbid, uuid=rid, disable_vectors=False)
        index_message = await IndexMessageBuilderV2(resource).full(reindex=True)
        # Drop previous documents first so chunks/embeddings that are no longer produced don't linger
        await datamanagers.fields.delete_processed_documents(txn, kbid=kbid, rid=rid)
        for field, chunks, embeddings in get_field_processed_contents(index_message):
            await datamanagers.fields.create_processed_documents(
                txn, field=field, chunks=chunks, embeddings=embeddings
            )
        await txn.commit()
        return True


async def rollover_kb(kbid: str, *, batch_size: int = 100, concurrency: int = 5) -> RolloverProgress:
    """
    Rewrite the field processed texts, chunks and embeddings documents of all resources of a KB.

    Safe to call again after a disruption: it resumes from the last checkpointed resource. At most
    one batch is redone, which is harmless as rewriting a resource is idempotent.
    """
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        raise TypeError("KB rollover requires MarkLogicDriver")
    storage = await get_storage()

    progress = await get_progress(driver, kbid)
    if progress is None:
        progress = RolloverProgress(started_at=datetime.now(timezone.utc))
        await _set_progress(driver, kbid, progress)
        logger.info("Starting KB rollover", extra={"kbid": kbid})
    else:
        logger.info(
            "Resuming KB rollover",
            extra={
                "kbid": kbid,
                "last_resource_id": progress.last_resource_id,
                "processed": progress.processed,
            },
        )

    semaphore = asyncio.Semaphore(concurrency)

    async def _rollover(rid: str) -> bool:
        async with semaphore:
            try:
                await rollover_resource(driver, storage, kbid, rid)
                return True
            except Exception:
                logger.exception("Error rolling over resource", extra={"kbid": kbid, "rid": rid})
                return False

    while True:
        async with driver.ro_transaction(kbid=kbid) as txn:
            rids = await datamanagers.resources.list_ids(
                txn, kbid=kbid, after_rid=progress.last_resource_id, limit=batch_size
            )
        if not rids:
            break

        results = await asyncio.gather(*(_rollover(rid) for rid in rids))
        progress.processed += len(results)
        progress.failed += results.count(False)
        progress.last_resource_id = rids[-1]
        await _set_progress(driver, kbid, progress)
        logger.info(
            "KB rollover progress",
            extra={"kbid": kbid, "processed": progress.processed, "failed": progress.failed},
        )

    await _delete_progress(driver, kbid)
    logger.info(
        "KB rollover finished",
        extra={"kbid": kbid, "processed": progress.processed, "failed": progress.failed},
    )
    return progress
