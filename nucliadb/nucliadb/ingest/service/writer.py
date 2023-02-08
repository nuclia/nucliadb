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
import traceback
import uuid
from io import BytesIO
from typing import AsyncIterator, Optional

from nucliadb_protos.knowledgebox_pb2 import (
    CleanedKnowledgeBoxResponse,
    DeleteKnowledgeBoxResponse,
    GCKnowledgeBoxResponse,
    KnowledgeBox,
    KnowledgeBoxID,
    KnowledgeBoxNew,
    KnowledgeBoxPrefix,
    KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate,
    Labels,
    NewKnowledgeBoxResponse,
    UpdateKnowledgeBoxResponse,
)
from nucliadb_protos.noderesources_pb2 import ShardCleaned
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BinaryData,
    BrokerMessage,
    CreateShadowShardRequest,
    DelEntitiesRequest,
    DeleteShadowShardRequest,
    DelLabelsRequest,
    DelVectorSetRequest,
    DetWidgetsRequest,
    ExportRequest,
    ExtractedVectorsWrapper,
    FileRequest,
    FileUploaded,
    GetEntitiesGroupRequest,
    GetEntitiesGroupResponse,
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelSetRequest,
    GetLabelSetResponse,
    GetLabelsRequest,
    GetLabelsResponse,
    GetVectorSetsRequest,
    GetVectorSetsResponse,
    GetWidgetRequest,
    GetWidgetResponse,
    GetWidgetsRequest,
    GetWidgetsResponse,
    IndexResource,
    IndexStatus,
    ListMembersRequest,
    ListMembersResponse,
    Member,
    OpStatusWriter,
    ResourceFieldExistsResponse,
    ResourceFieldId,
    ResourceIdRequest,
    ResourceIdResponse,
    SetEntitiesRequest,
    SetLabelsRequest,
    SetVectorSetRequest,
    SetVectorsRequest,
    SetVectorsResponse,
    SetWidgetsRequest,
    ShadowShardResponse,
)
from nucliadb_protos.writer_pb2 import Shards as PBShards
from nucliadb_protos.writer_pb2 import (
    UploadBinaryData,
    WriterStatusRequest,
    WriterStatusResponse,
)

from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.maindb.driver import TXNID
from nucliadb.ingest.orm import NODES
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxObj
from nucliadb.ingest.orm.processor import Processor
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.orm.shard import Shard
from nucliadb.ingest.orm.utils import get_node_klass
from nucliadb.ingest.settings import settings
from nucliadb.ingest.utils import get_driver
from nucliadb.sentry import SENTRY
from nucliadb_protos import writer_pb2_grpc
from nucliadb_utils.cache import KB_COUNTER_CACHE
from nucliadb_utils.keys import KB_SHARDS
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.utilities import (
    get_audit,
    get_cache,
    get_partitioning,
    get_storage,
    get_transaction,
)

if SENTRY:
    from sentry_sdk import capture_exception


class WriterServicer(writer_pb2_grpc.WriterServicer):
    def __init__(self):
        self.partitions = settings.partitions

    async def initialize(self):
        storage = await get_storage(service_name=SERVICE_NAME)
        audit = get_audit()
        driver = await get_driver()
        cache = await get_cache()
        self.proc = Processor(driver=driver, storage=storage, audit=audit, cache=cache)
        await self.proc.initialize()

    async def finalize(self):
        await self.proc.finalize()

    async def GetKnowledgeBox(self, request: KnowledgeBoxID, context=None) -> KnowledgeBox:  # type: ignore
        response: KnowledgeBox = await self.proc.get_kb(
            slug=request.slug, uuid=request.uuid
        )
        return response

    async def CleanAndUpgradeKnowledgeBoxIndex(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> CleanedKnowledgeBoxResponse:
        try:
            txn = await self.proc.driver.begin()
            node_klass = get_node_klass()
            all_shards = await node_klass.get_all_shards(txn, request.uuid)

            updated_shards = PBShards()
            updated_shards.CopyFrom(all_shards)

            for logic_shard in all_shards.shards:
                shard = node_klass.create_shard_klass(logic_shard.shard, logic_shard)
                replicas_cleaned = await shard.clean_and_upgrade()
                for replica_id, shard_cleaned in replicas_cleaned.items():
                    update_shards_with_updated_replica(
                        updated_shards, replica_id, shard_cleaned
                    )

            key = KB_SHARDS.format(kbid=request.uuid)
            await txn.set(key, updated_shards.SerializeToString())
            await txn.commit(resource=False)
            return CleanedKnowledgeBoxResponse()
        except Exception as e:
            if SENTRY:
                capture_exception(e)
            traceback.print_exc()
            await txn.abort()
            raise

    async def SetVectors(  # type: ignore
        self, request: SetVectorsRequest, context=None
    ) -> SetVectorsResponse:
        response = SetVectorsResponse()
        response.found = True

        txn = await self.proc.driver.begin()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()

        kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
        resobj = ResourceORM(txn, storage, kbobj, request.rid)

        field = await resobj.get_field(
            request.field.field, request.field.field_type, load=True
        )
        if field.value is None:
            await txn.abort()
            response.found = False
            return response

        evw = ExtractedVectorsWrapper()
        evw.field.CopyFrom(request.field)
        evw.vectors.CopyFrom(request.vectors)
        logger.debug(f"Setting {len(request.vectors.vectors.vectors)} vectors")

        try:
            await field.set_vectors(evw)
            await txn.commit(resource=False)
        except Exception as e:
            if SENTRY:
                capture_exception(e)
            traceback.print_exc()
            await txn.abort()

        return response

    async def NewKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxNew, context=None
    ) -> NewKnowledgeBoxResponse:
        try:
            kbid = await self.proc.create_kb(
                request.slug, request.config, request.forceuuid
            )
        except KnowledgeBoxConflict:
            return NewKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.CONFLICT)
        except Exception:
            logger.exception("Could not create KB", exc_info=True)
            return NewKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)
        return NewKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.OK, uuid=kbid)

    async def UpdateKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxUpdate, context=None
    ) -> UpdateKnowledgeBoxResponse:
        try:
            kbid = await self.proc.update_kb(request.uuid, request.slug, request.config)
        except KnowledgeBoxNotFound:
            return UpdateKnowledgeBoxResponse(
                status=KnowledgeBoxResponseStatus.NOTFOUND
            )
        except Exception:
            logger.exception("Could not create KB", exc_info=True)
            return UpdateKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)
        return UpdateKnowledgeBoxResponse(
            status=KnowledgeBoxResponseStatus.OK, uuid=kbid
        )

    async def DeleteKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> DeleteKnowledgeBoxResponse:
        try:
            await self.proc.delete_kb(request.uuid, request.slug)
        except KnowledgeBoxNotFound:
            logger.warning(f"KB not found: kbid={request.uuid}, slug={request.slug}")
        except Exception:
            logger.exception("Could not delete KB", exc_info=True)
            return DeleteKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)
        return DeleteKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.OK)

    async def ListKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxPrefix, context=None
    ) -> AsyncIterator[KnowledgeBoxID]:  # type: ignore
        async for slug in self.proc.list_kb(request.prefix):
            uuid = await self.proc.get_kb_uuid(slug)
            yield KnowledgeBoxID(uuid=uuid, slug=slug)

    async def GCKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> GCKnowledgeBoxResponse:
        response = GCKnowledgeBoxResponse()
        return response

    async def ProcessMessage(  # type: ignore
        self, request_stream: AsyncIterator[BrokerMessage], context=None
    ):
        response = OpStatusWriter()
        cache = await get_cache()
        async for message in request_stream:
            processed: bool = False
            try:
                processed = await self.proc.process(
                    message, -1, 0, transaction_check=False
                )
            except Exception:
                logger.exception("Error processing", stack_info=True)
                response.status = OpStatusWriter.Status.ERROR
                break
            response.status = OpStatusWriter.Status.OK
            logger.info(f"Processed {message.uuid}")
            if processed and cache is not None:
                await cache.delete(
                    KB_COUNTER_CACHE.format(kbid=message.kbid), invalidate=True
                )
        return response

    async def SetLabels(self, request: SetLabelsRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            try:
                await kbobj.set_labelset(request.id, request.labelset)
                await txn.commit(resource=False)
                response.status = OpStatusWriter.Status.OK
            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                traceback.print_exc()
                response.status = OpStatusWriter.Status.ERROR
                await txn.abort()
        else:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND
        return response

    async def DelLabels(self, request: DelLabelsRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            try:
                await kbobj.del_labelset(request.id)
                await txn.commit(resource=False)
                response.status = OpStatusWriter.Status.OK
            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                traceback.print_exc()
                response.status = OpStatusWriter.Status.ERROR
                await txn.abort()
        else:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND

        return response

    async def GetLabels(self, request: GetLabelsRequest, context=None) -> GetLabelsResponse:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        labels: Optional[Labels] = None
        if kbobj is not None:
            labels = await kbobj.get_labels()
        await txn.abort()
        response = GetLabelsResponse()
        if kbobj is None:
            response.status = GetLabelsResponse.Status.NOTFOUND
        else:
            response.kb.uuid = kbobj.kbid
            if labels is not None:
                response.labels.CopyFrom(labels)

        return response

    async def GetLabelSet(  # type: ignore
        self, request: GetLabelSetRequest, context=None
    ) -> GetLabelSetResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetLabelSetResponse()
        if kbobj is not None:
            await kbobj.get_labelset(request.labelset, response)
            response.kb.uuid = kbobj.kbid
            response.status = GetLabelSetResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetLabelSetResponse.Status.NOTFOUND
        return response

    async def GetVectorSets(  # type: ignore
        self, request: GetVectorSetsRequest, context=None
    ) -> GetVectorSetsResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetVectorSetsResponse()
        if kbobj is not None:
            await kbobj.get_vectorsets(response)
            response.kb.uuid = kbobj.kbid
            response.status = GetVectorSetsResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetVectorSetsResponse.Status.NOTFOUND
        return response

    async def DelVectorSet(  # type: ignore
        self, request: DelVectorSetRequest, context=None
    ) -> OpStatusWriter:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            await kbobj.del_vectorset(request.vectorset)
            response.status = OpStatusWriter.Status.OK
        await txn.commit(resource=False)
        if kbobj is None:
            response.status = OpStatusWriter.Status.NOTFOUND
        return response

    async def SetVectorSet(  # type: ignore
        self, request: SetVectorSetRequest, context=None
    ) -> OpStatusWriter:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            await kbobj.set_vectorset(request.id, request.vectorset)
            response.status = OpStatusWriter.Status.OK
        await txn.commit(resource=False)
        if kbobj is None:
            response.status = OpStatusWriter.Status.NOTFOUND
        return response

    async def GetEntities(  # type: ignore
        self, request: GetEntitiesRequest, context=None
    ) -> GetEntitiesResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetEntitiesResponse()
        if kbobj is not None:
            await kbobj.get_entities(response)
            response.kb.uuid = kbobj.kbid
            response.status = GetEntitiesResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetEntitiesResponse.Status.NOTFOUND
        return response

    async def GetEntitiesGroup(  # type: ignore
        self, request: GetEntitiesGroupRequest, context=None
    ) -> GetEntitiesGroupResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetEntitiesGroupResponse()
        if kbobj is not None:
            await kbobj.get_entitiesgroup(request.group, response)
            response.kb.uuid = kbobj.kbid
            response.status = GetEntitiesGroupResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetEntitiesGroupResponse.Status.NOTFOUND
        return response

    async def SetEntities(self, request: SetEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is None:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND
            return response
        await kbobj.set_entities(request.group, request.entities)
        response.status = OpStatusWriter.Status.OK
        await txn.commit(resource=False)
        return response

    async def DelEntities(self, request: DelEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            try:
                await kbobj.del_entities(request.group)
                await txn.commit(resource=False)
                response.status = OpStatusWriter.Status.OK
            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                traceback.print_exc()
                response.status = OpStatusWriter.Status.ERROR
                await txn.abort()
        else:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND
        return response

    async def GetWidget(  # type: ignore
        self, request: GetWidgetRequest, context=None
    ) -> GetWidgetResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetWidgetResponse()
        if kbobj is not None:
            await kbobj.get_widget(request.widget, response)
            response.kb.uuid = kbobj.kbid
            response.status = GetWidgetResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetWidgetResponse.Status.NOTFOUND
        return response

    async def GetWidgets(  # type: ignore
        self, request: GetWidgetsRequest, context=None
    ) -> GetWidgetsResponse:
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = GetWidgetsResponse()
        if kbobj is not None:
            await kbobj.get_widgets(response)
            response.kb.uuid = kbobj.kbid
            response.status = GetWidgetsResponse.Status.OK
        await txn.abort()
        if kbobj is None:
            response.status = GetWidgetsResponse.Status.NOTFOUND
        return response

    async def SetWidgets(self, request: SetWidgetsRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is None:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND
            return response
        await kbobj.set_widgets(request.widget)
        response.status = OpStatusWriter.Status.OK
        await txn.commit(resource=False)
        return response

    async def DelWidgets(self, request: DetWidgetsRequest, context=None) -> OpStatusWriter:  # type: ignore
        txn = await self.proc.driver.begin()
        kbobj = await self.proc.get_kb_obj(txn, request.kb)
        response = OpStatusWriter()
        if kbobj is not None:
            try:
                await kbobj.del_widgets(request.widget)
                await txn.commit(resource=False)
                response.status = OpStatusWriter.Status.OK
            except Exception as e:
                if SENTRY:
                    capture_exception(e)
                traceback.print_exc()
                response.status = OpStatusWriter.Status.ERROR
                await txn.abort()
        else:
            await txn.abort()
            response.status = OpStatusWriter.Status.NOTFOUND
        return response

    async def Status(  # type: ignore
        self, request: WriterStatusRequest, context=None
    ) -> WriterStatusResponse:
        logger.info("Status Call")
        response = WriterStatusResponse()
        txn = await self.proc.driver.begin()
        async for key in KnowledgeBoxObj.get_kbs(txn, slug="", count=-1):
            response.knowledgeboxes.append(key)

        for partition in settings.partitions:
            key = TXNID.format(worker=partition)
            msgid = await txn.get(key)
            if msgid is not None:
                response.msgid[partition] = msgid

        await txn.abort()
        return response

    async def ListMembers(  # type: ignore
        self, request: ListMembersRequest, context=None
    ) -> ListMembersResponse:
        response = ListMembersResponse()
        for nodeid, node in NODES.items():
            member = Member(
                id=str(nodeid),
                listen_address=node.address,
                type=node.type.to_pb(),
                load_score=node.load_score,
                shard_count=node.shard_count,
                dummy=node.dummy,
            )
            response.members.append(member)
        return response

    async def GetResourceId(  # type: ignore
        self, request: ResourceIdRequest, context=None
    ) -> ResourceIdResponse:
        response = ResourceIdResponse()
        response.uuid = ""
        txn = await self.proc.driver.begin()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()

        kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
        rid = await kbobj.get_resource_uuid_by_slug(request.slug)
        if rid:
            response.uuid = rid
        await txn.abort()
        return response

    async def ResourceFieldExists(  # type: ignore
        self, request: ResourceFieldId, context=None
    ) -> ResourceFieldExistsResponse:
        response = ResourceFieldExistsResponse()
        response.found = False
        resobj = None
        txn = await self.proc.driver.begin()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()

        kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
        resobj = ResourceORM(txn, storage, kbobj, request.rid)

        if request.field != "":
            field = await resobj.get_field(request.field, request.field_type, load=True)
            if field.value is not None:
                response.found = True
            else:
                response.found = False
            await txn.abort()
            return response

        if request.rid != "":
            if await resobj.exists():
                response.found = True
            else:
                response.found = False
            await txn.abort()
            return response

        if request.kbid != "":
            config = await KnowledgeBoxORM.get_kb(txn, request.kbid)
            if config is not None:
                response.found = True
            else:
                response.found = False
            await txn.abort()
            return response

        await txn.abort()
        return response

    async def Index(self, request: IndexResource, context=None) -> IndexStatus:  # type: ignore
        txn = await self.proc.driver.begin()
        storage = await get_storage(service_name=SERVICE_NAME)
        cache = await get_cache()

        kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
        resobj = ResourceORM(txn, storage, kbobj, request.rid)
        bm = await resobj.generate_broker_message()
        transaction = get_transaction()
        partitioning = get_partitioning()
        partition = partitioning.generate_partition(request.kbid, request.rid)
        await transaction.commit(bm, partition)

        response = IndexStatus()
        await txn.abort()
        return response

    async def ReIndex(self, request: IndexResource, context=None) -> IndexStatus:  # type: ignore
        try:
            txn = await self.proc.driver.begin()
            storage = await get_storage(service_name=SERVICE_NAME)
            cache = await get_cache()
            kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
            resobj = ResourceORM(txn, storage, kbobj, request.rid)
            resobj.disable_vectors = not request.reindex_vectors

            brain = await resobj.generate_index_message()
            shard_id = await kbobj.get_resource_shard_id(request.rid)
            shard: Optional[Shard] = None
            node_klass = get_node_klass()
            if shard_id is not None:
                shard = await kbobj.get_resource_shard(shard_id, node_klass)

            if shard is None:
                # Its a new resource
                # Check if we have enough resource to create a new shard
                shard = await node_klass.actual_shard(txn, request.kbid)
                if shard is None:
                    shard = await node_klass.create_shard_by_kbid(txn, request.kbid)

                await kbobj.set_resource_shard_id(request.rid, shard.sharduuid)

            if shard is not None:
                logger.info("Calling shard.add_resource")
                counter = await shard.add_resource(brain.brain, 0, uuid.uuid4().hex)
                logger.info("Finished shard.add_resource")

                if counter is not None and counter.fields > settings.max_node_fields:
                    shard = await node_klass.create_shard_by_kbid(txn, request.kbid)

            response = IndexStatus()
            await txn.abort()
            return response
        except Exception as e:
            if SENTRY:
                capture_exception(e)
            traceback.print_exc()
            await txn.abort()
            raise

    async def Export(self, request: ExportRequest, context=None):
        try:
            txn = await self.proc.driver.begin()
            storage = await get_storage(service_name=SERVICE_NAME)
            cache = await get_cache()

            kbobj = KnowledgeBoxORM(txn, storage, cache, request.kbid)
            async for resource in kbobj.iterate_resources():
                yield await resource.generate_broker_message()
            await txn.abort()
        except Exception:
            logger.exception("Export", stack_info=True)
            raise

    async def DownloadFile(self, request: FileRequest, context=None):
        storage = await get_storage(service_name=SERVICE_NAME)
        async for data in storage.download(request.bucket, request.key):
            yield BinaryData(data=data)

    async def UploadFile(self, request: AsyncIterator[UploadBinaryData], context=None) -> FileUploaded:  # type: ignore
        storage = await get_storage(service_name=SERVICE_NAME)
        data: UploadBinaryData

        destination: Optional[StorageField] = None
        cf = CloudFile()
        data = await request.__anext__()
        if data.HasField("metadata"):
            bucket = storage.get_bucket_name(data.metadata.kbid)
            destination = storage.field_klass(
                storage=storage, bucket=bucket, fullkey=data.metadata.key
            )
            cf.content_type = data.metadata.content_type
            cf.filename = data.metadata.filename
            cf.size = data.metadata.size
        else:
            raise AttributeError("Metadata not found")

        async def generate_buffer(
            storage: Storage, request: AsyncIterator[UploadBinaryData]  # type: ignore
        ):
            # Storage requires uploading chunks of a specified size, this is
            # why we need to have an intermediate buffer
            buf = BytesIO()
            async for chunk in request:
                if not chunk.HasField("payload"):
                    raise AttributeError("Payload not found")
                buf.write(chunk.payload)
                while buf.tell() > storage.chunk_size:
                    buf.seek(0)
                    data = buf.read(storage.chunk_size)
                    if len(data):
                        yield data
                    old_data = buf.read()
                    buf = BytesIO()
                    buf.write(old_data)
            buf.seek(0)
            data = buf.read()
            if len(data):
                yield data

        if destination is None:
            raise AttributeError("No destination file")
        await storage.uploaditerator(generate_buffer(storage, request), destination, cf)
        result = FileUploaded()
        return result

    async def CreateShadowShard(  # type: ignore
        self, request: CreateShadowShardRequest, context=None
    ) -> ShadowShardResponse:
        response = ShadowShardResponse(success=False)
        try:
            node_klass = get_node_klass()
            txn = await self.proc.driver.begin()
            await node_klass.create_shadow_shard(
                txn, request.kbid, request.node, request.replica.id
            )
            await txn.commit(resource=False)
            response.success = True
        except Exception as e:
            event_id: Optional[str] = None
            if SENTRY:
                event_id = capture_exception(e)
            logger.error(
                f"Error creating shadow shard. Check sentry for more details. Event id: {event_id}"
            )
            await txn.abort()
        return response

    async def DeleteShadowShard(  # type: ignore
        self, request: DeleteShadowShardRequest, context=None
    ) -> ShadowShardResponse:
        response = ShadowShardResponse(success=False)
        try:
            node_klass = get_node_klass()
            txn = await self.proc.driver.begin()
            await node_klass.delete_shadow_shard(txn, request.kbid, request.replica.id)
            await txn.commit(resource=False)
            response.success = True
        except Exception as exc:
            event_id: Optional[str] = None
            if SENTRY:
                event_id = capture_exception(exc)
            logger.error(
                f"Error deleting shadow shard. Check sentry for more details. Event id: {event_id}"
            )
            await txn.abort()
        return response


def update_shards_with_updated_replica(
    shards: PBShards, replica_id: str, updated_replica: ShardCleaned
):
    for logic_shard in shards.shards:
        for replica in logic_shard.replicas:
            if replica.shard.id == replica_id:
                replica.shard.document_service = updated_replica.document_service
                replica.shard.vector_service = updated_replica.vector_service
                replica.shard.paragraph_service = updated_replica.paragraph_service
                replica.shard.relation_service = updated_replica.relation_service
                return
