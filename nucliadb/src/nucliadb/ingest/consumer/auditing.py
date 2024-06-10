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
import uuid
from functools import partial

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb_protos import audit_pb2, nodereader_pb2, noderesources_pb2, writer_pb2
from nucliadb_utils import const
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.cache.pubsub import PubSubDriver
from nucliadb_utils.storages.storage import Storage

from . import metrics
from .utils import DelayedTaskHandler

logger = logging.getLogger(__name__)


AUDIT_TYPES = {
    writer_pb2.Notification.WriteType.CREATED: audit_pb2.AuditRequest.AuditType.NEW,
    writer_pb2.Notification.WriteType.MODIFIED: audit_pb2.AuditRequest.AuditType.MODIFIED,
    writer_pb2.Notification.WriteType.DELETED: audit_pb2.AuditRequest.AuditType.DELETED,
}


class IndexAuditHandler:
    """
    The purpose of this class is to handle the auditing of data
    that is indexed on the platform.
    """

    subscription_id: str
    loop: asyncio.AbstractEventLoop

    def __init__(
        self,
        *,
        audit: AuditStorage,
        pubsub: PubSubDriver,
        check_delay: float = 5.0,
    ):
        self.audit = audit
        self.pubsub = pubsub
        self.shard_manager = get_shard_manager()
        self.task_handler = DelayedTaskHandler(check_delay)

    async def initialize(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.subscription_id = str(uuid.uuid4())
        await self.task_handler.initialize()
        await self.pubsub.subscribe(
            handler=self.handle_message,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
            group="audit-shard-stats",
            subscription_id=self.subscription_id,
        )

    async def finalize(self) -> None:
        await self.pubsub.unsubscribe(self.subscription_id)
        await self.task_handler.finalize()

    async def handle_message(self, raw_data: bytes) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.action != writer_pb2.Notification.Action.INDEXED:
            # not a notification we care about
            metrics.total_messages.inc({"action": "ignored", "type": "audit_counter"})
            return

        self.task_handler.schedule(notification.kbid, partial(self.process_kb, notification.kbid))
        metrics.total_messages.inc({"action": "scheduled", "type": "audit_counter"})

    @metrics.handler_histo.wrap({"type": "audit_counter"})
    async def process_kb(self, kbid: str) -> None:
        try:
            shard_groups: list[writer_pb2.ShardObject] = await self.shard_manager.get_shards_by_kbid(
                kbid
            )
        except ShardsNotFound:
            logger.warning(f"No shards found for kbid {kbid}, skipping")
            return

        logger.info({"message": "Processing counter audit for kbid", "kbid": kbid})

        total_fields = 0
        total_paragraphs = 0

        for shard_obj in shard_groups:
            node, shard_id = choose_node(shard_obj)
            shard: nodereader_pb2.Shard = await node.reader.GetShard(
                nodereader_pb2.GetShardRequest(shard_id=noderesources_pb2.ShardId(id=shard_id))  # type: ignore
            )

            total_fields += shard.fields
            total_paragraphs += shard.paragraphs

        await self.audit.report(
            kbid=kbid,
            audit_type=audit_pb2.AuditRequest.AuditType.INDEXED,
            kb_counter=audit_pb2.AuditKBCounter(fields=total_fields, paragraphs=total_paragraphs),
        )


class ResourceWritesAuditHandler:
    """
    The purpose of this class is to handle the auditing
    of writes that went through ingest
    """

    subscription_id: str

    def __init__(
        self,
        *,
        storage: Storage,
        audit: AuditStorage,
        pubsub: PubSubDriver,
    ):
        self.storage = storage
        self.audit = audit
        self.pubsub = pubsub

    async def initialize(self) -> None:
        self.subscription_id = str(uuid.uuid4())
        await self.pubsub.subscribe(
            handler=self.handle_message,
            key=const.PubSubChannels.RESOURCE_NOTIFY.format(kbid="*"),
            group="audit-writes",
            subscription_id=self.subscription_id,
        )

    async def finalize(self) -> None:
        await self.pubsub.unsubscribe(self.subscription_id)

    async def handle_message(self, raw_data) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.write_type == notification.WriteType.UNSET:
            metrics.total_messages.inc({"action": "ignored", "type": "audit_fields"})
            return

        message_audit: writer_pb2.Audit = notification.message_audit
        if message_audit.message_source == writer_pb2.BrokerMessage.MessageSource.PROCESSOR:
            metrics.total_messages.inc({"action": "ignored", "type": "audit_fields"})
            return

        logger.info({"message": "Processing field audit for kbid", "kbid": notification.kbid})

        metrics.total_messages.inc({"action": "scheduled", "type": "audit_fields"})
        with metrics.handler_histo({"type": "audit_fields"}):
            when = message_audit.when if message_audit.HasField("when") else None
            await self.audit.report(
                kbid=message_audit.kbid,
                when=when,
                user=message_audit.user,
                rid=message_audit.uuid,
                origin=message_audit.origin,
                field_metadata=list(message_audit.field_metadata),
                audit_type=AUDIT_TYPES.get(notification.write_type),
                audit_fields=list(message_audit.audit_fields),
            )
