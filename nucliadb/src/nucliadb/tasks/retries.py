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
from enum import Enum
from typing import Optional

from pydantic import BaseModel

from nucliadb.common.context import ApplicationContext

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
        self.max_retries = max_retries
        self.context = context

    @property
    def metadata_key(self):
        return TASK_METADATA_MAINDB.format(
            kbid=self.kbid, task_type=self.task_type, task_id=self.task_id
        )

    async def get_metadata(self) -> Optional[TaskMetadata]:
        async with self.context.kv_driver.transaction(read_only=True) as txn:
            metadata = await txn.get(self.metadata_key)
            if metadata is None:
                return None
            return TaskMetadata.model_validate_json(metadata)

    async def set_metadata(self, metadata: TaskMetadata) -> None:
        async with self.context.kv_driver.transaction() as txn:
            await txn.set(self.metadata_key, metadata.model_dump_json().encode())
            await txn.commit()

    def wrap(self, func):
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
                )
                await self.set_metadata(metadata)

            if metadata.status in (TaskMetadata.Status.COMPLETED, TaskMetadata.Status.FAILED):
                logger.info(
                    f"{self.type} task is {metadata.status.value}. Skipping",
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
                await self.set_metadata(metadata)
                return
            try:
                metadata.status = TaskMetadata.Status.RUNNING
                func_result = await func(*args, **kwargs)
            except Exception as ex:
                metadata.retries += 1
                metadata.error_messages.append(str(ex))
                logger.info(
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
                await self.set_metadata(metadata)

        return wrapper
