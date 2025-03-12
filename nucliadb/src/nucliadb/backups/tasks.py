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
from typing import Awaitable, Callable

from nucliadb.backups.const import BackupsNatsConfig
from nucliadb.backups.create import backup_kb_task
from nucliadb.backups.delete import delete_backup_task
from nucliadb.backups.models import CreateBackupRequest, DeleteBackupRequest, RestoreBackupRequest
from nucliadb.backups.restore import restore_kb_task
from nucliadb.common.context import ApplicationContext
from nucliadb.tasks import create_consumer, create_producer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.producer import NatsTaskProducer


def creator_consumer() -> NatsTaskConsumer[CreateBackupRequest]:
    consumer: NatsTaskConsumer = create_consumer(
        name="backup_creator",
        stream=BackupsNatsConfig.stream,
        consumer=BackupsNatsConfig.create_consumer,
        callback=backup_kb_task,
        msg_type=CreateBackupRequest,
        max_concurrent_messages=10,
    )
    return consumer


async def create(kbid: str, backup_id: str) -> None:
    producer: NatsTaskProducer[CreateBackupRequest] = create_producer(
        name="backup_creator",
        stream=BackupsNatsConfig.stream,
        producer_subject=BackupsNatsConfig.create_consumer.subject,
        msg_type=CreateBackupRequest,
    )
    msg = CreateBackupRequest(
        kb_id=kbid,
        backup_id=backup_id,
    )
    await producer.send(msg)


def restorer_consumer() -> NatsTaskConsumer[RestoreBackupRequest]:
    consumer: NatsTaskConsumer = create_consumer(
        name="backup_restorer",
        stream=BackupsNatsConfig.stream,
        consumer=BackupsNatsConfig.restore_consumer,
        callback=restore_kb_task,
        msg_type=RestoreBackupRequest,
        max_concurrent_messages=10,
    )
    return consumer


async def restore(kbid: str, backup_id: str) -> None:
    producer: NatsTaskProducer[RestoreBackupRequest] = create_producer(
        name="backup_restorer",
        stream=BackupsNatsConfig.stream,
        producer_subject=BackupsNatsConfig.restore_consumer.subject,
        msg_type=RestoreBackupRequest,
    )
    msg = RestoreBackupRequest(
        kb_id=kbid,
        backup_id=backup_id,
    )
    await producer.send(msg)


def deleter_consumer() -> NatsTaskConsumer[DeleteBackupRequest]:
    consumer: NatsTaskConsumer[DeleteBackupRequest] = create_consumer(
        name="backup_deleter",
        stream=BackupsNatsConfig.stream,
        consumer=BackupsNatsConfig.delete_consumer,
        callback=delete_backup_task,
        msg_type=DeleteBackupRequest,
        max_concurrent_messages=2,
    )
    return consumer


async def delete(backup_id: str) -> None:
    producer: NatsTaskProducer[DeleteBackupRequest] = create_producer(
        name="backup_deleter",
        stream=BackupsNatsConfig.stream,
        producer_subject=BackupsNatsConfig.delete_consumer.subject,
        msg_type=DeleteBackupRequest,
    )
    msg = DeleteBackupRequest(
        backup_id=backup_id,
    )
    await producer.send(msg)


async def initialize_consumers(context: ApplicationContext) -> list[Callable[[], Awaitable[None]]]:
    creator = creator_consumer()
    restorer = restorer_consumer()
    deleter = deleter_consumer()
    await creator.initialize(context)
    await restorer.initialize(context)
    await deleter.initialize(context)
    return [
        creator.finalize,
        restorer.finalize,
        deleter.finalize,
    ]
