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
from enum import Enum
from typing import AsyncIterator, Dict, List, Optional, Tuple

from nucliadb_protos.audit_pb2 import AuditField, AuditRequest
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBox as KnowledgeBoxPB
from nucliadb_protos.knowledgebox_pb2 import (
    KnowledgeBoxConfig,
    KnowledgeBoxID,
    KnowledgeBoxResponseStatus,
    Widget,
)
from nucliadb_protos.train_pb2 import (
    GetFieldsRequest,
    GetParagraphsRequest,
    GetResourcesRequest,
    GetSentencesRequest,
    TrainField,
    TrainParagraph,
    TrainResource,
    TrainSentence,
)
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldType, Notification
from sentry_sdk import capture_exception

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.maindb.driver import Driver, Transaction
from nucliadb.ingest.orm.exceptions import DeadletteredError
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import KB_RESOURCE_SLUG_BASE, Resource
from nucliadb.ingest.orm.shard import Shard
from nucliadb.ingest.orm.utils import get_node_klass
from nucliadb.ingest.settings import settings
from nucliadb.sentry import SENTRY
from nucliadb_utils.audit.audit import AuditStorage
from nucliadb_utils.cache.utility import Cache
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_cache, get_storage

DEFAULT_WIDGET = Widget(id="dashboard", mode=Widget.WidgetMode.INPUT)
DEFAULT_WIDGET.features.useFilters = True
DEFAULT_WIDGET.features.suggestEntities = True
DEFAULT_WIDGET.features.suggestSentences = True
DEFAULT_WIDGET.features.suggestParagraphs = True
DEFAULT_WIDGET.features.suggestLabels = False
DEFAULT_WIDGET.features.editLabels = True
DEFAULT_WIDGET.features.entityAnnotation = True


class TxnResult(Enum):
    RESOURCE_CREATED = 0
    RESOURCE_MODIFIED = 1


AUDIT_TYPES: Dict[TxnResult, int] = {
    TxnResult.RESOURCE_CREATED: AuditRequest.AuditType.NEW,
    TxnResult.RESOURCE_MODIFIED: AuditRequest.AuditType.MODIFIED,
}


class Processor:
    messages: Dict[str, List[BrokerMessage]]

    def __init__(
        self,
        driver: Driver,
        storage: Storage,
        audit: Optional[AuditStorage] = None,
        cache: Optional[Cache] = None,
        partition: Optional[str] = None,
    ):
        self.messages = {}
        self.driver = driver
        self.storage = storage
        self.audit = audit
        self.partition = partition
        self.cache = cache

    async def initialize(self):
        await self.driver.initialize()
        if self.cache is not None:
            await self.cache.initialize()

    async def finalize(self):
        await self.driver.finalize()
        if self.cache is not None:
            await self.cache.finalize()

    @staticmethod
    def iterate_auditable_fields(resource_keys, message):
        """
        Generator that emits the combined list of field ids from both
        the existing resource and message that needs to be considered
        in the audit of fields.
        """
        yielded = set()

        # Include all fields present in the message we are processing
        for field_id in message.files.keys():
            key = (field_id, FieldType.FILE)
            yield key
            yielded.add(key)

        for field_id in message.conversations.keys():
            key = (field_id, FieldType.CONVERSATION)
            yield key
            yielded.add(key)

        for field_id in message.layouts.keys():
            key = (field_id, FieldType.LAYOUT)
            yield key
            yielded.add(key)

        for field_id in message.texts.keys():
            key = (field_id, FieldType.TEXT)
            yield key
            yielded.add(key)

        for field_id in message.keywordsets.keys():
            key = (field_id, FieldType.KEYWORDSET)
            yield key
            yielded.add(key)

        for field_id in message.datetimes.keys():
            key = (field_id, FieldType.DATETIME)
            yield key
            yielded.add(key)

        for field_id in message.links.keys():
            key = (field_id, FieldType.LINK)
            yield key
            yielded.add(key)

        # Include fields of the current resource
        # We'll Ignore fields that are not in current message with the exception of DELETE resource
        # message, then we want them all, because among other things we need to report all the individual
        # sizes that disappear from storage

        for field_type, field_id in resource_keys:
            if field_type is FieldType.GENERIC:
                continue

            if not (
                field_id in message.files
                or message.type is BrokerMessage.MessageType.DELETE
            ):
                continue

            # Avoid duplicates
            if (field_type, field_id) in yielded:
                continue

            yield (field_id, field_type)

    async def collect_audit_fields(self, message: BrokerMessage) -> List[AuditField]:
        audit_storage_fields: List[AuditField] = []
        txn = await self.driver.begin()

        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()
        kb = KnowledgeBox(txn, storage, cache, message.kbid)
        resource = Resource(txn, storage, kb, message.uuid)
        field_keys = await resource.get_fields_ids()

        for field_id, field_type in self.iterate_auditable_fields(field_keys, message):
            field = await resource.get_field(field_id, field_type, load=True)
            val = await field.get_value()

            auditfield = AuditField()
            auditfield.field_type = field_type
            auditfield.field_id = field_id
            if field_type is FieldType.FILE:
                auditfield.filename = message.files[field_id].file.filename
            if val is None:
                # The field did not exist previously, so we are adding it now
                auditfield.action = AuditField.FieldAction.ADDED
                if field_type is FieldType.FILE:
                    auditfield.size = auditfield.size_delta = message.files[
                        field_id
                    ].file.size
            elif message.type is BrokerMessage.MessageType.DELETE:
                # The file did exist, and we are deleting the field as a side effect of deleting the resource
                auditfield.action = AuditField.FieldAction.DELETED
                if field_type is FieldType.FILE:
                    auditfield.size = 0
                    auditfield.size_delta = -val.file.size
            else:
                # The field did exist, so we are overwriting it, with a modified file
                # in case of a file
                auditfield.action = AuditField.FieldAction.MODIFIED
                if field_type is FieldType.FILE:
                    auditfield.size = message.files[field_id].file.size
                    auditfield.size_delta = (
                        val.file.size - message.files[field_id].file.size
                    )

            audit_storage_fields.append(auditfield)

        if (
            message.delete_fields
            and message.type is not BrokerMessage.MessageType.DELETE
        ):
            # If we are fully deleting a resource we won't iterate the delete_fields (if any)
            # Make no sense as we already collected all resource fields as deleted
            for fieldid in message.delete_fields:
                field = await resource.get_field(
                    fieldid.field, FieldType.FILE, load=True
                )
                audit_field = AuditField()
                audit_field.action = AuditField.FieldAction.DELETED
                audit_field.field_id = fieldid.field
                audit_field.field_type = fieldid.field_type
                if fieldid.field_type is FieldType.FILE:
                    val = await field.get_value()
                    audit_field.size = 0
                    audit_field.size_delta = -val.file.size
                    audit_field.filename = val.file.filename
                audit_storage_fields.append(audit_field)

        await txn.abort()
        return audit_storage_fields

    async def process(
        self,
        message: BrokerMessage,
        seqid: int,
        partition: Optional[str] = None,
        transaction_check: bool = True,
    ) -> bool:
        partition = partition if self.partition is None else self.partition
        if partition is None:
            raise AttributeError()

        # When running in transactional mode, we need to check that
        # that the current message doesn't violate the sequence order for the
        # current partition
        if transaction_check:
            last_seqid = await self.driver.last_seqid(partition)
            if last_seqid is not None and seqid <= last_seqid:
                return False

        audit_type: Optional[int] = None
        audit_fields = None
        if message.type == BrokerMessage.MessageType.DELETE:
            audit_fields = await self.collect_audit_fields(message)
            await self.delete_resource(message, seqid, partition)
            audit_type = AuditRequest.AuditType.DELETED
        elif message.type == BrokerMessage.MessageType.AUTOCOMMIT:
            audit_fields = await self.collect_audit_fields(message)
            txn_result = await self.autocommit(message, seqid, partition)
            audit_type = AUDIT_TYPES.get(txn_result)
        elif message.type == BrokerMessage.MessageType.MULTI:
            await self.multi(message, seqid)
        elif message.type == BrokerMessage.MessageType.COMMIT:
            audit_fields = await self.collect_audit_fields(message)
            txn_result = await self.commit(message, seqid, partition)
            audit_type = AUDIT_TYPES.get(txn_result)
        elif message.type == BrokerMessage.MessageType.ROLLBACK:
            await self.rollback(message, seqid, partition)

        # There are some operations that doesn't require audit report by definition
        # like rollback or multi and others because there was no action executed for
        # some reason. This is signaled as audit_type == None
        if self.audit is not None and audit_type is not None:
            await self.audit.report(message, audit_type, audit_fields=audit_fields)
        elif self.audit is None:
            logger.warning("No audit defined")
        elif audit_type is None:
            logger.warning(f"Audit type empty txn_result: {txn_result}")
        return True

    async def get_resource_uuid(self, kb: KnowledgeBox, message: BrokerMessage) -> str:
        if message.uuid is None:
            uuid = await kb.get_resource_uuid_by_slug(message.slug)
        else:
            uuid = message.uuid
        return uuid

    async def delete_resource(self, message: BrokerMessage, seqid: int, partition: str):
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, message.kbid)

        uuid = await self.get_resource_uuid(kb, message)
        shard_id = await kb.get_resource_shard_id(uuid)
        if shard_id is None:
            logger.warn(f"Resource {uuid} does not exist")
        else:
            node_klass = get_node_klass()
            shard: Optional[Shard] = await kb.get_resource_shard(shard_id, node_klass)
            if shard is None:
                raise AttributeError("Shard not available")
            await shard.delete_resource(message.uuid, seqid)
            try:
                await kb.delete_resource(message.uuid)
            except Exception as exc:
                await txn.abort()
                await self.notify_abort(
                    partition, seqid, message.multiid, message.kbid, message.uuid
                )
                raise exc
        if txn.open:
            await txn.commit(partition, seqid)
        await self.notify_commit(
            partition, seqid, message.multiid, message.kbid, message.uuid
        )

    def generate_index(self, resource: Resource, messages: List[BrokerMessage]):
        pass

    async def txn(
        self, messages: List[BrokerMessage], seqid: int, partition: str
    ) -> Optional[TxnResult]:
        if len(messages) == 0:
            return None

        txn = await self.driver.begin()
        kbid = messages[0].kbid
        if not await KnowledgeBox.exist_kb(txn, kbid):
            logger.warning(f"KB {kbid} is deleted: skiping txn")
            await txn.commit(partition, seqid)
            return None

        multi = messages[0].multiid
        kb = KnowledgeBox(txn, self.storage, self.cache, kbid)
        uuid = await self.get_resource_uuid(kb, messages[0])
        resource: Optional[Resource] = None
        handled_exception = None
        origin_txn = seqid

        created = False

        try:
            for message in messages:
                if resource is not None:
                    assert resource.uuid == message.uuid
                result = await self.apply_resource(message, kb, resource)

                if result is None:
                    continue

                resource, _created = result
                created = created or _created

            if resource:
                await resource.compute_global_text()
                await resource.compute_global_tags(resource.indexer)

            if resource and resource.modified:
                shard_id = await kb.get_resource_shard_id(uuid)
                shard: Optional[Shard] = None
                node_klass = get_node_klass()

                if shard_id is not None:
                    shard = await kb.get_resource_shard(shard_id, node_klass)

                if shard is None:
                    # Its a new resource
                    # Check if we have enough resource to create a new shard
                    shard = await node_klass.actual_shard(txn, kbid)
                    if shard is None:
                        shard = await node_klass.create_shard_by_kbid(txn, kbid)
                    await kb.set_resource_shard_id(uuid, shard.sharduuid)

                if shard is not None:
                    count = await shard.add_resource(resource.indexer.brain, seqid)
                    if count > settings.max_node_fields:
                        shard = await node_klass.create_shard_by_kbid(txn, kbid)

                else:
                    raise AttributeError("Shard is not available")

                await txn.commit(partition, seqid)

                # Slug may have conflicts as its not partitioned properly. We make it as short as possible
                txn = await self.driver.begin()
                resource.txn = txn
                await resource.set_slug()
                await txn.commit(resource=False)

                await self.notify_commit(partition, origin_txn, multi, kbid, uuid)

            elif resource and resource.modified is False:
                await txn.abort()
                await self.notify_abort(partition, origin_txn, multi, kbid, uuid)
                logger.warn(f"This message did not modified resource")
        except Exception as exc:
            # As we are in the middle of a transaction, we cannot let the exception raise directly
            # as we need to do some cleanup. Exception will be reraised at the end of the function
            # and then handled by the top caller, so errors can be handled in the same place.
            await self.deadletter(messages, partition, seqid)
            await self.notify_abort(partition, origin_txn, multi, kbid, uuid)
            handled_exception = exc
        finally:
            if resource is not None:
                resource.clean()
            # tx should be already commited or aborted, but in the event of an exception
            # it could be left open. Make sure to close it it's still open
            if txn.open:
                await txn.abort()

        if handled_exception is not None:
            if seqid == -1:
                raise handled_exception
            else:
                raise DeadletteredError() from handled_exception

        return TxnResult.RESOURCE_CREATED if created else TxnResult.RESOURCE_MODIFIED

    async def autocommit(self, message: BrokerMessage, seqid: int, partition: str):
        return await self.txn([message], seqid, partition)

    async def multi(self, message: BrokerMessage, seqid: int):
        self.messages.setdefault(message.multiid, []).append(message)

    async def commit(self, message: BrokerMessage, seqid: int, partition: str):
        if message.multiid not in self.messages:
            # Error
            logger.error(f"Closed multi {message.multiid}")
            await self.deadletter([message], partition, seqid)
            return
        else:
            return await self.txn(self.messages[message.multiid], seqid, partition)

    async def rollback(self, message: BrokerMessage, seqid: int, partition: str):
        # Error
        logger.error(f"Closed multi {message.multiid}")
        del self.messages[message.multiid]
        await self.notify_abort(
            partition, seqid, message.multiid, message.kbid, message.uuid
        )

    async def deadletter(
        self, messages: List[BrokerMessage], partition: str, seqid: int
    ):
        for seq, message in enumerate(messages):
            await self.storage.deadletter(message, seq, seqid, partition)

    async def apply_resource(
        self,
        message: BrokerMessage,
        kb: KnowledgeBox,
        resource: Optional[Resource] = None,
    ) -> Optional[Tuple[Resource, bool]]:
        created = False
        if resource is None:
            # Make sure we load the resource in case it already exists on db
            if message.uuid is None and message.slug:
                uuid = await kb.get_resource_uuid_by_slug(message.slug)
            else:
                uuid = message.uuid
            resource = await kb.get(uuid)

        if resource is None and message.source is message.MessageSource.WRITER:
            # It's a new resource
            resource = await kb.add_resource(uuid, message.slug, message.basic)
            created = True
        elif resource is not None:
            # It's an update of an existing resource, can come either from writer or
            # from processing
            if message.HasField("basic") or message.slug != "":
                await resource.set_basic(message.basic, slug=message.slug)
        elif resource is None and message.source is message.MessageSource.PROCESSOR:
            # It's a new resource, and somehow we received the message coming from processing before
            # the "fast" one, this shouldn't happen
            logger.info(
                f"Secondary message for resource {message.uuid} and resource does not exist, ignoring"
            )
            return None

        if message.origin and resource:
            await resource.set_origin(message.origin)

        if resource:
            await resource.apply_fields(message)
            await resource.apply_extracted(message)
            return (resource, created)

        return None

    async def notify_commit(
        self, partition: str, seqid: int, multi: str, kbid: str, uuid: str
    ):
        message = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=uuid,
            kbid=kbid,
            action=Notification.COMMIT,
        )
        await self.notify(f"notify.{kbid}", message.SerializeToString())

    async def notify_abort(
        self, partition: str, seqid: int, multi: str, kbid: str, uuid: str
    ):
        message = Notification(
            partition=int(partition),
            seqid=seqid,
            multi=multi,
            uuid=uuid,
            kbid=kbid,
            action=Notification.ABORT,
        )
        await self.notify(f"notify.{kbid}", message.SerializeToString())

    # KB tools

    async def get_kb_obj(
        self, txn: Transaction, kbid: KnowledgeBoxID
    ) -> Optional[KnowledgeBox]:
        uuid: Optional[str] = kbid.uuid
        if uuid == "":
            uuid = await KnowledgeBox.get_kb_uuid(txn, kbid.slug)

        if uuid is None:
            return None

        if not (await KnowledgeBox.exist_kb(txn, uuid)):
            return None

        storage = await get_storage()
        cache = await get_cache()
        kbobj = KnowledgeBox(txn, storage, cache, uuid)
        return kbobj

    async def get_kb(self, slug: str = "", uuid: Optional[str] = "") -> KnowledgeBoxPB:
        txn = await self.driver.begin()

        if uuid == "" and slug != "":
            uuid = await KnowledgeBox.get_kb_uuid(txn, slug)

        response = KnowledgeBoxPB()
        if uuid is None:
            response.status = KnowledgeBoxResponseStatus.NOTFOUND
            await txn.abort()
            return response

        config = await KnowledgeBox.get_kb(txn, uuid)

        await txn.abort()

        if config is None:
            response.status = KnowledgeBoxResponseStatus.NOTFOUND
            return response

        response.uuid = uuid
        response.slug = config.slug
        response.config.CopyFrom(config)
        return response

    async def get_kb_uuid(self, slug: str) -> Optional[str]:
        txn = await self.driver.begin()
        uuid = await KnowledgeBox.get_kb_uuid(txn, slug)
        await txn.abort()
        return uuid

    async def create_kb(
        self,
        slug: str,
        config: Optional[KnowledgeBoxConfig],
        forceuuid: Optional[str] = None,
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid, failed = await KnowledgeBox.create(
                txn, slug, config=config, uuid=forceuuid
            )
        except Exception as e:
            await txn.abort()
            if SENTRY:
                capture_exception(e)
            raise e

        if not failed:
            storage = await get_storage(service_name=SERVICE_NAME)
            cache = await get_cache()
            kb = KnowledgeBox(txn, storage, cache, uuid)
            await kb.set_widgets(DEFAULT_WIDGET)

        if failed:
            await txn.abort()
            raise Exception("Failed to create KB")
        else:
            await txn.commit(resource=False)
        return uuid

    async def update_kb(
        self, kbid: str, slug: str, config: Optional[KnowledgeBoxConfig]
    ) -> str:
        txn = await self.driver.begin()
        try:
            uuid = await KnowledgeBox.update(txn, kbid, slug, config=config)
        except Exception as e:
            await txn.abort()
            raise e
        await txn.commit(resource=False)
        return uuid

    async def list_kb(self, prefix: str):
        txn = await self.driver.begin()
        async for slug in KnowledgeBox.get_kbs(txn, prefix):
            yield slug
        await txn.abort()

    async def delete_kb(self, kbid: str = "", slug: str = "") -> str:
        txn = await self.driver.begin()
        uuid = await KnowledgeBox.delete_kb(txn, kbid=kbid, slug=slug)
        await txn.commit(resource=False)
        return uuid

    async def notify(self, channel, payload: bytes):
        if self.cache is not None and self.cache.pubsub is not None:
            await self.cache.pubsub.publish(channel, payload)

    async def kb_sentences(
        self, request: GetSentencesRequest
    ) -> AsyncIterator[TrainSentence]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for sentence in resource.iterate_sentences(request.metadata):
                    yield sentence
        else:
            async for resource in kb.iterate_resources():
                async for sentence in resource.iterate_sentences(request.metadata):
                    yield sentence
        await txn.abort()

    async def kb_paragraphs(
        self, request: GetParagraphsRequest
    ) -> AsyncIterator[TrainParagraph]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for paragraph in resource.iterate_paragraphs(request.metadata):
                    yield paragraph
        else:
            async for resource in kb.iterate_resources():
                async for paragraph in resource.iterate_paragraphs(request.metadata):
                    yield paragraph
        await txn.abort()

    async def kb_fields(self, request: GetFieldsRequest) -> AsyncIterator[TrainField]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        if request.uuid != "":
            # Filter by uuid
            resource = await kb.get(request.uuid)
            if resource:
                async for field in resource.iterate_fields(request.metadata):
                    yield field
        else:
            async for resource in kb.iterate_resources():
                async for field in resource.iterate_fields(request.metadata):
                    yield field
        await txn.abort()

    async def kb_resources(
        self, request: GetResourcesRequest
    ) -> AsyncIterator[TrainResource]:
        txn = await self.driver.begin()
        kb = KnowledgeBox(txn, self.storage, self.cache, request.kb.uuid)
        base = KB_RESOURCE_SLUG_BASE.format(kbid=request.kb.uuid)
        async for key in txn.keys(match=base, count=-1):
            # Fetch and Add wanted item
            rid = await txn.get(key)
            if rid is not None:
                resource = await kb.get(rid.decode())
                if resource is not None:
                    yield await resource.get_resource(request.metadata)

        await txn.abort()
