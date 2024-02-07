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

from nucliadb_protos.resources_pb2 import FieldType

from nucliadb.common.cluster.exceptions import ShardsNotFound
from nucliadb.common.cluster.manager import choose_node
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.maindb.driver import Driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
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
        driver: Driver,
        audit: AuditStorage,
        pubsub: PubSubDriver,
        check_delay: float = 5.0,
    ):
        self.driver = driver
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

        self.task_handler.schedule(
            notification.kbid, partial(self.process_kb, notification.kbid)
        )
        metrics.total_messages.inc({"action": "scheduled", "type": "audit_counter"})

    @metrics.handler_histo.wrap({"type": "audit_counter"})
    async def process_kb(self, kbid: str) -> None:
        try:
            shard_groups: list[
                writer_pb2.ShardObject
            ] = await self.shard_manager.get_shards_by_kbid(kbid)
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
            kb_counter=audit_pb2.AuditKBCounter(
                fields=total_fields, paragraphs=total_paragraphs
            ),
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
        driver: Driver,
        storage: Storage,
        audit: AuditStorage,
        pubsub: PubSubDriver,
    ):
        self.driver = driver
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

    def iterate_auditable_fields(
        self,
        resource_keys: list[tuple[FieldType.ValueType, str]],
        message: writer_pb2.BrokerMessage,
    ):
        """
        Generator that emits the combined list of field ids from both
        the existing resource and message that needs to be considered
        in the audit of fields.
        """
        yielded = set()

        # Include all fields present in the message we are processing
        for field_id in message.files.keys():
            key = (field_id, writer_pb2.FieldType.FILE)
            yield key
            yielded.add(key)

        for field_id in message.conversations.keys():
            key = (field_id, writer_pb2.FieldType.CONVERSATION)
            yield key
            yielded.add(key)

        for field_id in message.layouts.keys():
            key = (field_id, writer_pb2.FieldType.LAYOUT)
            yield key
            yielded.add(key)

        for field_id in message.texts.keys():
            key = (field_id, writer_pb2.FieldType.TEXT)
            yield key
            yielded.add(key)

        for field_id in message.keywordsets.keys():
            key = (field_id, writer_pb2.FieldType.KEYWORDSET)
            yield key
            yielded.add(key)

        for field_id in message.datetimes.keys():
            key = (field_id, writer_pb2.FieldType.DATETIME)
            yield key
            yielded.add(key)

        for field_id in message.links.keys():
            key = (field_id, writer_pb2.FieldType.LINK)
            yield key
            yielded.add(key)

        for field_type, field_id in resource_keys:
            if field_type is writer_pb2.FieldType.GENERIC:
                continue

            if not (
                field_id in message.files
                or message.type is writer_pb2.BrokerMessage.MessageType.DELETE
            ):
                continue

            # Avoid duplicates
            if (field_type, field_id) in yielded:
                continue

            yield (field_id, field_type)

    async def collect_audit_fields(
        self, message: writer_pb2.BrokerMessage
    ) -> list[audit_pb2.AuditField]:
        if message.type == writer_pb2.BrokerMessage.MessageType.DELETE:
            # If we are fully deleting a resource we won't iterate the delete_fields (if any).
            # Make no sense as we already collected all resource fields as deleted
            return []

        audit_storage_fields: list[audit_pb2.AuditField] = []
        async with self.driver.transaction() as txn:
            kb = KnowledgeBox(txn, self.storage, message.kbid)
            resource = Resource(txn, self.storage, kb, message.uuid)
            field_keys = await resource.get_fields_ids()

            for field_id, field_type in self.iterate_auditable_fields(
                field_keys, message
            ):
                auditfield = audit_pb2.AuditField()
                auditfield.field_type = field_type
                auditfield.field_id = field_id
                if field_type is writer_pb2.FieldType.FILE:
                    auditfield.filename = message.files[field_id].file.filename
                # The field did exist, so we are overwriting it, with a modified file
                # in case of a file
                auditfield.action = audit_pb2.AuditField.FieldAction.MODIFIED
                if field_type is writer_pb2.FieldType.FILE:
                    auditfield.size = message.files[field_id].file.size

                audit_storage_fields.append(auditfield)

            for fieldid in message.delete_fields or []:
                field = await resource.get_field(
                    fieldid.field, writer_pb2.FieldType.FILE, load=True
                )
                audit_field = audit_pb2.AuditField()
                audit_field.action = audit_pb2.AuditField.FieldAction.DELETED
                audit_field.field_id = fieldid.field
                audit_field.field_type = fieldid.field_type
                if fieldid.field_type is writer_pb2.FieldType.FILE:
                    val = await field.get_value()
                    audit_field.size = 0
                    if val is not None:
                        audit_field.filename = val.file.filename
                audit_storage_fields.append(audit_field)

        return audit_storage_fields

    async def handle_message(self, raw_data) -> None:
        data = self.pubsub.parse(raw_data)
        notification = writer_pb2.Notification()
        notification.ParseFromString(data)

        if notification.write_type == notification.WriteType.UNSET:
            metrics.total_messages.inc({"action": "ignored", "type": "audit_fields"})
            return

        message = notification.message
        if message.source == message.MessageSource.PROCESSOR:
            metrics.total_messages.inc({"action": "ignored", "type": "audit_fields"})
            return

        logger.info(
            {"message": "Processing field audit for kbid", "kbid": notification.kbid}
        )

        metrics.total_messages.inc({"action": "scheduled", "type": "audit_fields"})
        with metrics.handler_histo({"type": "audit_fields"}):
            audit_fields = await self.collect_audit_fields(message)
            field_metadata = [fi.field for fi in message.field_metadata]
            when = message.audit.when if message.audit.HasField("when") else None
            await self.audit.report(
                kbid=message.kbid,
                when=when,
                user=message.audit.user,
                rid=message.uuid,
                origin=message.audit.origin,
                field_metadata=field_metadata,
                audit_type=AUDIT_TYPES.get(notification.write_type),
                audit_fields=audit_fields,
            )
