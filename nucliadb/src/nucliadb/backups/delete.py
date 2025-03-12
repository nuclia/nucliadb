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
import logging

from nucliadb.backups.const import StorageKeys
from nucliadb.backups.models import DeleteBackupRequest
from nucliadb.backups.settings import settings
from nucliadb.common.context import ApplicationContext

logger = logging.getLogger(__name__)


async def delete_backup_task(context: ApplicationContext, msg: DeleteBackupRequest):
    """
    Deletes the backup files from the cloud storage.
    """
    await delete_backup(context, msg.backup_id)


async def delete_backup(context: ApplicationContext, backup_id: str):
    total_deleted = 0
    while True:
        deleted = await delete_n(context, backup_id, n=1000)
        if deleted == 0:
            # No more objects to delete
            break
        total_deleted += deleted
        logger.info(f"Deleted {total_deleted} objects from backup", extra={"backup_id": backup_id})
    logger.info(f"Backup deletion completed", extra={"backup_id": backup_id})


async def delete_n(context: ApplicationContext, backup_id: str, n: int):
    concurrent_batch_size = 50
    deleted = 0
    tasks = []
    async for object_info in context.blob_storage.iterate_objects(
        bucket=settings.backups_bucket,
        prefix=StorageKeys.BACKUP_PREFIX.format(backup_id=backup_id),
    ):
        if deleted >= n:
            # Deleted enough objects
            break
        tasks.append(
            asyncio.create_task(
                context.blob_storage.delete_upload(
                    uri=object_info.name,
                    bucket_name=settings.backups_bucket,
                )
            )
        )
        deleted += 1
        if len(tasks) > concurrent_batch_size:
            await asyncio.gather(*tasks)
            tasks = []
    if len(tasks) > 0:
        await asyncio.gather(*tasks)
        tasks = []
    return deleted
