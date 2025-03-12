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
import functools
import logging
from datetime import datetime, timezone
from enum import Enum
from typing import Callable, Optional, cast

from pydantic import BaseModel

from nucliadb.common.context import ApplicationContext
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.pg import PGDriver, PGTransaction

logger = logging.getLogger(__name__)

TASK_METADATA_MAINDB = "/kbs/{kbid}/tasks/{task_type}/{task_id}"


class TaskMetadata(BaseModel):
    class Status(Enum):
        RUNNING = "running"
        FAILED = "failed"
        COMPLETED = "completed"

    task_id: str
    status: Status
    retries: int = 0
    error_messages: list[str] = []
    last_modified: Optional[datetime] = None


class TaskRetryHandler:
    """
    Class that wraps a task consumer function and handles retries by storing metadata in the KV store.

    Example:

        retry_handler = TaskRetryHandler(
            kbid="kbid",
            task_type="work",
            task_id="task_id",
            context=context,
        )

        @retry_handler.wrap
        async def my_task_consumer_func(kbid: str, task_id: str):
            pass

    """

    def __init__(
        self,
        kbid: str,
        task_type: str,
        task_id: str,
        context: ApplicationContext,
        max_retries: int = 5,
    ):
        self.kbid = kbid
        self.task_type = task_type
        self.task_id = task_id
        # Limit max retries to 50
        self.max_retries = min(max_retries, 50)
        self.context = context

    @property
    def metadata_key(self):
        return TASK_METADATA_MAINDB.format(
            kbid=self.kbid, task_type=self.task_type, task_id=self.task_id
        )

    async def get_metadata(self) -> Optional[TaskMetadata]:
        return await _get_metadata(self.context.kv_driver, self.metadata_key)

    async def set_metadata(self, metadata: TaskMetadata) -> None:
        await _set_metadata(self.context.kv_driver, self.metadata_key, metadata)

    def wrap(self, func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            func_result = None
            metadata = await self.get_metadata()
            if metadata is None:
                # Task is not scheduled yet
                metadata = TaskMetadata(
                    task_id=self.task_id,
                    status=TaskMetadata.Status.RUNNING,
                    retries=0,
                    last_modified=datetime.now(timezone.utc),
                )
                await self.set_metadata(metadata)

            if metadata.status in (TaskMetadata.Status.COMPLETED, TaskMetadata.Status.FAILED):
                logger.info(
                    f"{self.task_type} task is {metadata.status.value}. Skipping",
                    extra={"kbid": self.kbid, "task_type": self.task_type, "task_id": self.task_id},
                )
                return

            if metadata.retries >= self.max_retries:
                metadata.status = TaskMetadata.Status.FAILED
                metadata.error_messages.append("Max retries reached")
                logger.info(
                    f"Task reached max retries. Setting to FAILED state",
                    extra={"kbid": self.kbid, "task_type": self.task_type, "task_id": self.task_id},
                )
                metadata.last_modified = datetime.now(timezone.utc)
                await self.set_metadata(metadata)
                return
            try:
                metadata.status = TaskMetadata.Status.RUNNING
                func_result = await func(*args, **kwargs)
            except Exception as ex:
                metadata.retries += 1
                metadata.error_messages.append(str(ex))
                logger.exception(
                    f"Task failed. Will be retried",
                    extra={"kbid": self.kbid, "task_type": self.task_type, "task_id": self.task_id},
                )
                raise
            else:
                logger.info(
                    f"Task finished successfully",
                    extra={"kbid": self.kbid, "task_type": self.task_type, "task_id": self.task_id},
                )
                metadata.status = TaskMetadata.Status.COMPLETED
                return func_result
            finally:
                metadata.last_modified = datetime.now(timezone.utc)
                await self.set_metadata(metadata)

        return wrapper


async def _get_metadata(kv_driver: Driver, metadata_key: str) -> Optional[TaskMetadata]:
    async with kv_driver.transaction(read_only=True) as txn:
        metadata = await txn.get(metadata_key)
        if metadata is None:
            return None
        return TaskMetadata.model_validate_json(metadata)


async def _set_metadata(kv_driver: Driver, metadata_key: str, metadata: TaskMetadata) -> None:
    async with kv_driver.transaction() as txn:
        await txn.set(metadata_key, metadata.model_dump_json().encode())
        await txn.commit()


async def purge_metadata(kv_driver: Driver) -> int:
    """
    Purges old task metadata records that are in a final state and older than 15 days.
    Returns the total number of records purged.
    """
    if not isinstance(kv_driver, PGDriver):
        return 0

    total_purged = 0
    start: Optional[str] = ""
    while True:
        start, purged = await purge_batch(kv_driver, start)
        total_purged += purged
        if start is None:
            break
    return total_purged


async def purge_batch(
    kv_driver: PGDriver, start: Optional[str] = None, batch_size: int = 200
) -> tuple[Optional[str], int]:
    """
    Returns the next start key and the number of purged records. If start is None, it means there are no more records to purge.
    """
    async with kv_driver.transaction() as txn:
        txn = cast(PGTransaction, txn)
        async with txn.connection.cursor() as cur:
            await cur.execute(
                """
                SELECT key from resources
                WHERE key  ~ '^/kbs/[^/]*/tasks/[^/]*/[^/]*$'
                AND key > %s
                ORDER BY key
                LIMIT %s
                """,
                (start, batch_size),
            )
            records = await cur.fetchall()
            keys = [r[0] for r in records]

    if not keys:
        # No more records to purge
        return None, 0

    to_delete = []
    for key in keys:
        metadata = await _get_metadata(kv_driver, key)
        if metadata is None:  # pragma: no cover
            continue
        task_finished = metadata.status in (TaskMetadata.Status.COMPLETED, TaskMetadata.Status.FAILED)
        old_task = (
            metadata.last_modified is None
            or (datetime.now(timezone.utc) - metadata.last_modified).days >= 15
        )
        if task_finished and old_task:
            to_delete.append(key)

    n_to_delete = len(to_delete)
    delete_batch_size = 50
    while len(to_delete) > 0:
        batch = to_delete[:delete_batch_size]
        to_delete = to_delete[delete_batch_size:]
        async with kv_driver.transaction() as txn:
            for key in batch:
                logger.info("Purging task metadata", extra={"key": key})
                await txn.delete(key)
            await txn.commit()
    return keys[-1], n_to_delete
