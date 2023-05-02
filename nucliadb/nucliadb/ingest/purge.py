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

import pkg_resources

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.maindb.driver import Driver
from nucliadb.ingest.orm.exceptions import NodeError, ShardNotFound
from nucliadb.ingest.orm.knowledgebox import (
    KB_TO_DELETE,
    KB_TO_DELETE_BASE,
    KB_TO_DELETE_STORAGE_BASE,
    KnowledgeBox,
)
from nucliadb.ingest.utils import get_driver
from nucliadb_telemetry import errors
from nucliadb_telemetry.logs import setup_logging
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage


async def purge_kb(driver: Driver):
    logger.info("START PURGING KB")
    async for key in driver.keys(match=KB_TO_DELETE_BASE, count=-1):
        logger.info(f"Purging kb {key}")
        try:
            kbid = key.split("/")[2]
        except Exception:
            logger.info(
                f"  X Skipping purge {key}, wrong key format, expected {KB_TO_DELETE_BASE}"
            )
            continue

        try:
            await KnowledgeBox.purge(driver, kbid)
            logger.info(f"  √ Successfully Purged {kbid}")
        except ShardNotFound as exc:
            errors.capture_exception(exc)
            logger.info(
                f"  X At least one shard was unavailable while purging {kbid}, skipping"
            )
            continue
        except NodeError as exc:
            errors.capture_exception(exc)
            logger.info(
                f"  X At least one node was unavailable while purging {kbid}, skipping"
            )
            continue

        except Exception as exc:
            errors.capture_exception(exc)
            logger.info(
                f"  X ERROR while executing KnowledgeBox.purge of {kbid}, skipping: {exc.__class__.__name__} {exc}"
            )
            continue

        # Now delete the tikv delete mark
        try:
            txn = await driver.begin()
            key_to_purge = KB_TO_DELETE.format(kbid=kbid)
            await txn.delete(key_to_purge)
            await txn.commit(resource=False)
            logger.info(f"  √ Deleted {key_to_purge}")
        except Exception as exc:
            errors.capture_exception(exc)
            logger.info(f"  X Error while deleting key {key_to_purge}")
            await txn.abort()
    logger.info("END PURGING KB")


async def purge_kb_storage(driver: Driver, storage: Storage):
    # Last iteration deleted all kbs, and set their storages marked to be deleted also in tikv
    # Here we'll delete those storage buckets
    logger.info("START PURGING KB STORAGE")
    async for key in driver.keys(match=KB_TO_DELETE_STORAGE_BASE, count=-1):
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
            logger.info(
                f"  . Nothing was deleted for {key}, (Bucket not yet empty), will try next time"
            )
        elif not deleted:
            logger.info(
                f"  ! Expected bucket for {key} was not found, will delete marker"
            )
            delete_marker = True
        elif deleted:
            logger.info(f"  √ Bucket successfully deleted")
            delete_marker = True

        if delete_marker:
            try:
                txn = await driver.begin()
                await txn.delete(key)
                logger.info(f"  √ Deleted storage deletion marker {key}")
            except Exception as exc:
                errors.capture_exception(exc)
                logger.info(f"  X Error while deleting key {key}")
                await txn.abort()
            else:
                await txn.commit(resource=False)

    logger.info("FINISH PURGING KB STORAGE")


async def main():
    # Clean up all kb marked to delete
    driver = await get_driver()
    storage = await get_storage(
        gcs_scopes=["https://www.googleapis.com/auth/devstorage.full_control"],
        service_name=SERVICE_NAME,
    )

    await purge_kb(driver)
    await purge_kb_storage(driver, storage)
    await storage.finalize()


def run() -> int:  # pragma: no cover
    setup_logging()

    errors.setup_error_handling(pkg_resources.get_distribution("nucliadb").version)

    return asyncio.run(main())
