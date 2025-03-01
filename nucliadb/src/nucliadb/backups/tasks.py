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

from nucliadb.backups.creator import backup_kb
from nucliadb.backups.models import CreateBackupRequest
from nucliadb.common.context import ApplicationContext
from nucliadb.tasks import create_consumer, create_producer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.producer import NatsTaskProducer

logger = logging.getLogger(__name__)


class Backup:
    @classmethod
    async def creator_consumer(cls) -> NatsTaskConsumer[CreateBackupRequest]:
        consumer: NatsTaskConsumer = create_consumer(
            name="backup_creator",
            stream="backups",
            stream_subjects=["backups.>"],
            consumer_subject="backups.create",
            callback=backup_kb,
            msg_type=CreateBackupRequest,
            max_concurrent_messages=10,
        )
        return consumer

    @classmethod
    async def create(cls, kbid: str, backup_id: str) -> None:
        producer: NatsTaskProducer[CreateBackupRequest] = create_producer(
            name="backup_creator",
            stream="backups",
            stream_subjects=["backups.>"],
            producer_subject="backups.create",
            msg_type=CreateBackupRequest,
        )
        msg = CreateBackupRequest(
            kbid=kbid,
            backup_id=backup_id,
        )
        await producer.send(msg)



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
    def __init__(
        self,
        task_id: str,
        context: ApplicationContext,
        max_tries: int = 5,
    ):
        self.task_id = task_id
        self.max_tries = max_tries
        self.context = context

    async def get_metadata(self) -> Optional[TaskMetadata]:
        async with self.context.kv_driver.transaction(read_only=True) as txn:
            metadata = await txn.get(f"/tasks/{self.task_id}")
            if metadata is None:
                return None
            return TaskMetadata.model_validate_json(metadata)

    async def set_metadata(self, metadata: TaskMetadata) -> None:
        async with self.context.kv_driver.transaction() as txn:
            await txn.set(f"/tasks/{self.task_id}", metadata.model_dump_json())
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
                    extra={"task_id": self.task_id}
                )
                return

            if metadata.retries >= self.max_tries:
                metadata.status = TaskMetadata.Status.FAILED
                metadata.error_messages.append("Max retries reached")
                logger.info(
                    f"Task reached max retries. Setting to FAILED state",
                    extra={"task_id": self.task_id}
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
                    extra={"task_id": self.task_id}
                )
                raise
            else:
                logger.info(
                    f"Task finished successfully",
                    extra={"task_id": self.task_id}
                )
                metadata.status = TaskMetadata.Status.COMPLETED
                return func_result
            finally:
                await self.set_metadata(metadata)

        return wrapper
