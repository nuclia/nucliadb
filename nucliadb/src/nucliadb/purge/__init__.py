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
import asyncio
import importlib.metadata
from typing import AsyncGenerator

from nucliadb.common.cluster.exceptions import NodeError, ShardNotFound
from nucliadb.common.cluster.utils import setup_cluster, teardown_cluster
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.utils import setup_driver, teardown_driver
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.knowledgebox import (
    KB_TO_DELETE,
    KB_TO_DELETE_BASE,
    KB_TO_DELETE_STORAGE_BASE,
    KB_VECTORSET_TO_DELETE,
    KB_VECTORSET_TO_DELETE_BASE,
    RESOURCE_TO_DELETE_STORAGE_BASE,
    KnowledgeBox,
)
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage


async def _iter_keys(driver: Driver, match: str) -> AsyncGenerator[str, None]:
    async with driver.transaction(read_only=True) as keys_txn:
        async for key in keys_txn.keys(match=match):
            yield key


async def purge_kb(driver: Driver):
    logger.info("START PURGING KB")
    async for key in _iter_keys(driver, KB_TO_DELETE_BASE):
        logger.info(f"Purging kb {key}")
        try:
            kbid = key.split("/")[2]
        except Exception:
            logger.warning(f"  X Skipping purge {key}, wrong key format, expected {KB_TO_DELETE_BASE}")
            continue

        try:
            await KnowledgeBox.purge(driver, kbid)
            logger.info(f"  √ Successfully Purged {kbid}")
        except ShardNotFound as exc:
            errors.capture_exception(exc)
            logger.error(f"  X At least one shard was unavailable while purging {kbid}, skipping")
            continue
        except NodeError as exc:
            errors.capture_exception(exc)
            logger.error(f"  X At least one node was unavailable while purging {kbid}, skipping")
            continue

        except Exception as exc:
            errors.capture_exception(exc)
            logger.error(
                f"  X ERROR while executing KnowledgeBox.purge of {kbid}, skipping: {exc.__class__.__name__} {exc}"
            )
            continue

        # Now delete the tikv delete mark
        try:
            async with driver.transaction() as txn:
                key_to_purge = KB_TO_DELETE.format(kbid=kbid)
                await txn.delete(key_to_purge)
                await txn.commit()
            logger.info(f"  √ Deleted {key_to_purge}")
        except Exception as exc:
            errors.capture_exception(exc)
            logger.error(f"  X Error while deleting key {key_to_purge}")
            await txn.abort()
    logger.info("END PURGING KB")


async def purge_kb_storage(driver: Driver, storage: Storage):
    # Last iteration deleted all kbs, and set their storages marked to be deleted also in tikv
    # Here we'll delete those storage buckets
    logger.info("START PURGING KB STORAGE")
    async for key in _iter_keys(driver, KB_TO_DELETE_STORAGE_BASE):
        logger.info(f"Purging storage {key}")
        try:
            kbid = key.split("/")[2]
        except Exception:
            logger.info(
                f"  X Skipping purge {key}, wrong key format, expected {KB_TO_DELETE_STORAGE_BASE}"
            )
            continue

        deleted, conflict = await storage.delete_kb(kbid)

        delete_marker = False
        if conflict:
            logger.info(f"  . Nothing was deleted for {key}, (Bucket not yet empty), will try next time")
            # Just in case something failed while setting a lifecycle policy to
            # remove all elements from the bucket, reschedule it
            await storage.schedule_delete_kb(kbid)
        elif not deleted:
            logger.info(f"  ! Expected bucket for {key} was not found, will delete marker")
            delete_marker = True
        elif deleted:
            logger.info("  √ Bucket successfully deleted")
            delete_marker = True

        if delete_marker:
            try:
                async with driver.transaction() as txn:
                    await txn.delete(key)
                    await txn.commit()
                logger.info(f"  √ Deleted storage deletion marker {key}")
            except Exception as exc:
                errors.capture_exception(exc)
                logger.info(f"  X Error while deleting key {key}")

    logger.info("FINISH PURGING KB STORAGE")


async def purge_deleted_resource_storage(driver: Driver, storage: Storage) -> None:
    """
    Remove from storage all resources marked as deleted.

    Returns the number of resources purged.
    """
    logger.info("Starting purge of deleted resource storage")
    to_purge = await _count_resources_storage_to_purge(driver)
    logger.info(f"Found {to_purge} resources to purge")
    while True:
        try:
            purged = await _purge_resources_storage_batch(driver, storage, batch_size=100)
            if not purged:
                logger.info("No more resources to purge found")
                return
            logger.info(f"Purged {purged} resources")

        except asyncio.CancelledError:
            logger.info("Purge of deleted resource storage was cancelled")
            return


async def _count_resources_storage_to_purge(driver: Driver) -> int:
    """
    Count the number of resources marked as deleted in storage.
    """
    async with driver.transaction(read_only=True) as txn:
        return await txn.count(match=RESOURCE_TO_DELETE_STORAGE_BASE)


async def _purge_resources_storage_batch(driver: Driver, storage: Storage, batch_size: int = 100) -> int:
    """
    Remove from storage a batch of resources marked as deleted. Returns the
    number of resources purged.
    """
    # Get the keys of the resources to delete in batches of 100
    to_delete_batch = []
    async with driver.transaction(read_only=True) as txn:
        async for key in txn.keys(match=RESOURCE_TO_DELETE_STORAGE_BASE, count=batch_size):
            to_delete_batch.append(key)

    if not to_delete_batch:
        return 0

    # Delete the resources blobs from storage
    logger.info(f"Purging {len(to_delete_batch)} deleted resources")
    tasks = []
    for key in to_delete_batch:
        kbid, resource_id = key.split("/")[-2:]
        tasks.append(asyncio.create_task(storage.delete_resource(kbid, resource_id)))
    await asyncio.gather(*tasks)

    # Delete the schedule-to-delete keys
    async with driver.transaction() as txn:
        for key in to_delete_batch:
            await txn.delete(key)
        await txn.commit()

    return len(to_delete_batch)


async def purge_kb_vectorsets(driver: Driver, storage: Storage):
    """Vectors for a vectorset are stored in a key inside each resource. Iterate
    through all resources of the KB and remove any storage object containing
    vectors for the specific vectorset to purge.

    """
    logger.info("START PURGING KB VECTORSETS")

    purged = []
    async for key in _iter_keys(driver, KB_VECTORSET_TO_DELETE_BASE):
        logger.info(f"Purging vectorsets {key}")
        try:
            _base, kbid, vectorset = key.lstrip("/").split("/")
        except ValueError:
            logger.info(f"  X Skipping purge {key}, wrong key format, expected {KB_VECTORSET_TO_DELETE}")
            continue

        try:
            async with driver.transaction(read_only=True) as txn:
                kb = KnowledgeBox(txn, storage, kbid)
                async for resource in kb.iterate_resources():
                    fields = await resource.get_fields(force=True)
            # we don't need the maindb transaction anymore to remove vectors from storage
            for field in fields.values():
                await field.delete_vectors(vectorset)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.error(
                f"  X ERROR while executing KB vectorset purge, skipping",
                exc_info=exc,
                extra={"kbid": kbid},
            )
            continue

        purged.append(key)

    async with driver.transaction() as txn:
        for key in purged:
            await txn.delete(key)
        await txn.commit()

    logger.info("FINISH PURGING KB VECTORSETS")


async def main():
    """
    This script will purge all knowledge boxes marked to be deleted in maindb.
    """
    await setup_cluster()
    driver = await setup_driver()
    storage = await get_storage(
        gcs_scopes=["https://www.googleapis.com/auth/devstorage.full_control"],
        service_name=SERVICE_NAME,
    )
    try:
        purge_resources_storage_task = asyncio.create_task(
            purge_deleted_resource_storage(driver, storage)
        )
        await purge_kb(driver)
        await purge_kb_storage(driver, storage)
        await purge_kb_vectorsets(driver, storage)
        await purge_resources_storage_task
    except Exception as ex:  # pragma: no cover
        logger.exception("Unhandled exception on purge command")
        errors.capture_exception(ex)
    finally:
        try:
            purge_resources_storage_task.cancel()
            await storage.finalize()
            await teardown_driver()
            await teardown_cluster()
        except Exception:  # pragma: no cover
            logger.exception("Error tearing down utilities on purge command")
            pass


def run() -> int:  # pragma: no cover
    setup_logging()
    errors.setup_error_handling(importlib.metadata.distribution("nucliadb").version)
    return asyncio.run(main())
