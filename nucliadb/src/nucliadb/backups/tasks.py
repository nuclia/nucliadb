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
from nucliadb.backups.create import backup_kb_retried
from nucliadb.backups.models import CreateBackupRequest
from nucliadb.tasks import create_consumer, create_producer
from nucliadb.tasks.consumer import NatsTaskConsumer
from nucliadb.tasks.producer import NatsTaskProducer


class Backup:
    @classmethod
    async def creator_consumer(cls) -> NatsTaskConsumer[CreateBackupRequest]:
        consumer: NatsTaskConsumer = create_consumer(
            name="backup_creator",
            stream="backups",
            stream_subjects=["backups.>"],
            consumer_subject="backups.create",
            callback=backup_kb_retried,
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
