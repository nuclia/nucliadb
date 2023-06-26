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
    SemanticModelMetadata,
    UpdateKnowledgeBoxResponse,
)
from nucliadb_protos.noderesources_pb2 import ShardCleaned
from nucliadb_protos.resources_pb2 import CloudFile
from nucliadb_protos.writer_pb2 import (
    BinaryData,
    BrokerMessage,
    DelEntitiesRequest,
    DelLabelsRequest,
    DelVectorSetRequest,
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
    GetSynonymsResponse,
    GetVectorSetsRequest,
    GetVectorSetsResponse,
    IndexResource,
    IndexStatus,
    ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse,
    ListMembersRequest,
    ListMembersResponse,
    NewEntitiesGroupRequest,
    NewEntitiesGroupResponse,
    OpStatusWriter,
    ResourceFieldExistsResponse,
    ResourceFieldId,
    ResourceIdRequest,
    ResourceIdResponse,
    SetEntitiesRequest,
    SetLabelsRequest,
    SetSynonymsRequest,
    SetVectorSetRequest,
    SetVectorsRequest,
    SetVectorsResponse,
)
from nucliadb_protos.writer_pb2 import Shards as PBShards
from nucliadb_protos.writer_pb2 import (
    UpdateEntitiesGroupRequest,
    UpdateEntitiesGroupResponse,
    UploadBinaryData,
    WriterStatusRequest,
    WriterStatusResponse,
)

from nucliadb.common.cluster.exceptions import AlreadyExists, EntitiesGroupNotFound
from nucliadb.common.cluster.manager import clean_and_upgrade, get_index_nodes
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.maindb.driver import Transaction
from nucliadb.common.maindb.utils import setup_driver
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict, KnowledgeBoxNotFound
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxObj
from nucliadb.ingest.orm.processor import Processor, sequence_manager
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.settings import settings
from nucliadb_protos import writer_pb2, writer_pb2_grpc
from nucliadb_telemetry import errors
from nucliadb_utils.cache import KB_COUNTER_CACHE
from nucliadb_utils.keys import KB_SHARDS
from nucliadb_utils.storages.storage import Storage, StorageField
from nucliadb_utils.utilities import (
    get_cache,
    get_partitioning,
    get_storage,
    get_transaction_utility,
)


class WriterServicer(writer_pb2_grpc.WriterServicer):
    def __init__(self):
        self.partitions = settings.partitions

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        self.driver = await setup_driver()
        self.cache = await get_cache()
        self.proc = Processor(
            driver=self.driver, storage=self.storage, cache=self.cache
        )
        self.shards_manager = get_shard_manager()

    async def finalize(self):
        ...

    async def GetKnowledgeBox(self, request: KnowledgeBoxID, context=None) -> KnowledgeBox:  # type: ignore
        response: KnowledgeBox = await self.proc.get_kb(
            slug=request.slug, uuid=request.uuid
        )
        return response

    async def CleanAndUpgradeKnowledgeBoxIndex(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> CleanedKnowledgeBoxResponse:
        try:
            async with self.driver.transaction() as txn:
                all_shards = await self.shards_manager.get_all_shards(txn, request.uuid)

                updated_shards = PBShards()
                updated_shards.CopyFrom(all_shards)

                for logic_shard in all_shards.shards:
                    replicas_cleaned = await clean_and_upgrade(logic_shard)
                    for replica_id, shard_cleaned in replicas_cleaned.items():
                        update_shards_with_updated_replica(
                            updated_shards, replica_id, shard_cleaned
                        )

                key = KB_SHARDS.format(kbid=request.uuid)
                await txn.set(key, updated_shards.SerializeToString())
                await txn.commit()
                return CleanedKnowledgeBoxResponse()
        except Exception as e:
            errors.capture_exception(e)
            logger.error("Error in ingest gRPC servicer", exc_info=True)
            raise

    async def SetVectors(  # type: ignore
        self, request: SetVectorsRequest, context=None
    ) -> SetVectorsResponse:
        response = SetVectorsResponse()
        response.found = True

        async with self.driver.transaction() as txn:
            kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
            resobj = ResourceORM(txn, self.storage, kbobj, request.rid)

            field = await resobj.get_field(
                request.field.field, request.field.field_type, load=True
            )
            if field.value is None:
                response.found = False
                return response

            evw = ExtractedVectorsWrapper()
            evw.field.CopyFrom(request.field)
            evw.vectors.CopyFrom(request.vectors)
            logger.debug(f"Setting {len(request.vectors.vectors.vectors)} vectors")

            try:
                await field.set_vectors(evw)
                await txn.commit()
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)

            return response

    async def NewKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxNew, context=None
    ) -> NewKnowledgeBoxResponse:
        try:
            kbid = await self.proc.create_kb(
                request.slug,
                request.config,
                parse_model_metadata(request),
                forceuuid=request.forceuuid,
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
        async for message in request_stream:
            try:
                await self.proc.process(
                    message, -1, partition=self.partitions[0], transaction_check=False
                )
            except Exception:
                logger.exception("Error processing", stack_info=True)
                response.status = OpStatusWriter.Status.ERROR
                break
            response.status = OpStatusWriter.Status.OK
            logger.info(f"Processed {message.uuid}")
            if self.cache is not None:
                await self.cache.delete(
                    KB_COUNTER_CACHE.format(kbid=message.kbid), invalidate=True
                )
        return response

    async def SetLabels(self, request: SetLabelsRequest, context=None) -> OpStatusWriter:  # type: ignore
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = OpStatusWriter()
            if kbobj is not None:
                try:
                    await kbobj.set_labelset(request.id, request.labelset)
                    await txn.commit()
                    response.status = OpStatusWriter.Status.OK
                except Exception as e:
                    errors.capture_exception(e)
                    logger.error("Error in ingest gRPC servicer", exc_info=True)
                    response.status = OpStatusWriter.Status.ERROR
            else:
                response.status = OpStatusWriter.Status.NOTFOUND
            return response

    async def DelLabels(self, request: DelLabelsRequest, context=None) -> OpStatusWriter:  # type: ignore
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = OpStatusWriter()
            if kbobj is not None:
                try:
                    await kbobj.del_labelset(request.id)
                    await txn.commit()
                    response.status = OpStatusWriter.Status.OK
                except Exception as e:
                    errors.capture_exception(e)
                    logger.error("Error in ingest gRPC servicer", exc_info=True)
                    response.status = OpStatusWriter.Status.ERROR
            else:
                response.status = OpStatusWriter.Status.NOTFOUND

            return response

    async def GetLabels(self, request: GetLabelsRequest, context=None) -> GetLabelsResponse:  # type: ignore
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            labels: Optional[Labels] = None
            if kbobj is not None:
                labels = await kbobj.get_labels()
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
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = GetLabelSetResponse()
            if kbobj is not None:
                await kbobj.get_labelset(request.labelset, response)
                response.kb.uuid = kbobj.kbid
                response.status = GetLabelSetResponse.Status.OK
            else:
                response.status = GetLabelSetResponse.Status.NOTFOUND
            return response

    async def GetVectorSets(  # type: ignore
        self, request: GetVectorSetsRequest, context=None
    ) -> GetVectorSetsResponse:
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = GetVectorSetsResponse()
            if kbobj is not None:
                await kbobj.get_vectorsets(response)
                response.kb.uuid = kbobj.kbid
                response.status = GetVectorSetsResponse.Status.OK
            else:
                response.status = GetVectorSetsResponse.Status.NOTFOUND
            return response

    async def DelVectorSet(  # type: ignore
        self, request: DelVectorSetRequest, context=None
    ) -> OpStatusWriter:
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = OpStatusWriter()
            if kbobj is not None:
                await kbobj.del_vectorset(request.vectorset)
                response.status = OpStatusWriter.Status.OK
                await txn.commit()
            else:
                response.status = OpStatusWriter.Status.NOTFOUND
            return response

    async def SetVectorSet(  # type: ignore
        self, request: SetVectorSetRequest, context=None
    ) -> OpStatusWriter:
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            response = OpStatusWriter()
            if kbobj is not None:
                await kbobj.set_vectorset(request.id, request.vectorset)
                response.status = OpStatusWriter.Status.OK
                await txn.commit()
            else:
                response.status = OpStatusWriter.Status.NOTFOUND
            return response

    async def NewEntitiesGroup(  # type: ignore
        self, request: NewEntitiesGroupRequest, context=None
    ) -> NewEntitiesGroupResponse:
        response = NewEntitiesGroupResponse()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            if kbobj is None:
                response.status = NewEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.create_entities_group(
                    request.group, request.entities
                )
            except AlreadyExists:
                response.status = NewEntitiesGroupResponse.Status.ALREADY_EXISTS
                return response

            await txn.commit()
            response.status = NewEntitiesGroupResponse.Status.OK
            return response

    async def GetEntities(  # type: ignore
        self, request: GetEntitiesRequest, context=None
    ) -> GetEntitiesResponse:
        response = GetEntitiesResponse()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)

            if kbobj is None:
                response.status = GetEntitiesResponse.Status.NOTFOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.get_entities(response)
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = GetEntitiesResponse.Status.ERROR
            else:
                response.kb.uuid = kbobj.kbid
                response.status = GetEntitiesResponse.Status.OK
            return response

    async def ListEntitiesGroups(  # type: ignore
        self, request: ListEntitiesGroupsRequest, context=None
    ) -> ListEntitiesGroupsResponse:
        response = ListEntitiesGroupsResponse()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)

            if kbobj is None:
                response.status = ListEntitiesGroupsResponse.Status.NOTFOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                entities_groups = await entities_manager.list_entities_groups()
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = ListEntitiesGroupsResponse.Status.ERROR
            else:
                response.status = ListEntitiesGroupsResponse.Status.OK
                for name, group in entities_groups.items():
                    response.groups[name].CopyFrom(group)

            return response

    async def GetEntitiesGroup(  # type: ignore
        self, request: GetEntitiesGroupRequest, context=None
    ) -> GetEntitiesGroupResponse:
        response = GetEntitiesGroupResponse()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)

            if kbobj is None:
                response.status = GetEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                entities_group = await entities_manager.get_entities_group(
                    request.group
                )
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = GetEntitiesGroupResponse.Status.ERROR
            else:
                if entities_group is None:
                    response.status = (
                        GetEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND
                    )
                else:
                    response.kb.uuid = kbobj.kbid
                    response.status = GetEntitiesGroupResponse.Status.OK
                    response.group.CopyFrom(entities_group)

            return response

    async def SetEntities(self, request: SetEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        response = OpStatusWriter()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.set_entities_group(
                    request.group, request.entities
                )
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = OpStatusWriter.Status.ERROR
            else:
                response.status = OpStatusWriter.Status.OK
                await txn.commit()
            return response

    async def UpdateEntitiesGroup(  # type: ignore
        self, request: UpdateEntitiesGroupRequest, context=None
    ) -> UpdateEntitiesGroupResponse:
        response = UpdateEntitiesGroupResponse()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            if kbobj is None:
                response.status = UpdateEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)

            try:
                await entities_manager.set_entities_group_metadata(
                    request.group,
                    title=request.title,
                    color=request.color,
                )
                updates = {**request.add, **request.update}
                await entities_manager.update_entities(request.group, updates)
                await entities_manager.delete_entities(request.group, request.delete)  # type: ignore
            except EntitiesGroupNotFound:
                response.status = (
                    UpdateEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND
                )
                return response

            await txn.commit()
            response.status = UpdateEntitiesGroupResponse.Status.OK
            return response

    async def DelEntities(self, request: DelEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        response = OpStatusWriter()
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.delete_entities_group(request.group)
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = OpStatusWriter.Status.ERROR
            else:
                await txn.commit()
                response.status = OpStatusWriter.Status.OK
            return response

    async def GetSynonyms(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> GetSynonymsResponse:
        kbid = request
        response = GetSynonymsResponse()
        txn: Transaction
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, kbid)
            if kbobj is None:
                response.status.status = OpStatusWriter.Status.NOTFOUND
                return response
            try:
                await kbobj.get_synonyms(response.synonyms)
                response.status.status = OpStatusWriter.Status.OK
                return response
            except Exception as e:
                errors.capture_exception(e)
                logger.exception("Errors getting synonyms")
                response.status.status = OpStatusWriter.Status.ERROR
                return response

    async def SetSynonyms(  # type: ignore
        self, request: SetSynonymsRequest, context=None
    ) -> OpStatusWriter:
        kbid = request.kbid
        response = OpStatusWriter()
        txn: Transaction
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, kbid)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response
            try:
                await kbobj.set_synonyms(request.synonyms)
                await txn.commit()
                response.status = OpStatusWriter.Status.OK
                return response
            except Exception as e:
                errors.capture_exception(e)
                logger.exception("Errors setting synonyms")
                response.status = OpStatusWriter.Status.ERROR
                return response

    async def DelSynonyms(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> OpStatusWriter:
        kbid = request
        response = OpStatusWriter()
        txn: Transaction
        async with self.driver.transaction() as txn:
            kbobj = await self.proc.get_kb_obj(txn, kbid)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response
            try:
                await kbobj.delete_synonyms()
                await txn.commit()
                response.status = OpStatusWriter.Status.OK
                return response
            except Exception as e:
                errors.capture_exception(e)
                logger.exception("Errors deleting synonyms")
                response.status = OpStatusWriter.Status.ERROR
                return response

    async def Status(  # type: ignore
        self, request: WriterStatusRequest, context=None
    ) -> WriterStatusResponse:
        logger.info("Status Call")
        response = WriterStatusResponse()
        async with self.driver.transaction() as txn:
            async for (_, slug) in KnowledgeBoxObj.get_kbs(txn, slug="", count=-1):
                response.knowledgeboxes.append(slug)

            for partition in settings.partitions:
                seq_id = await sequence_manager.get_last_seqid(self.driver, partition)
                if seq_id is not None:
                    response.msgid[partition] = seq_id

            return response

    async def ListMembers(  # type: ignore
        self, request: ListMembersRequest, context=None
    ) -> ListMembersResponse:
        response = ListMembersResponse()
        response.members.extend(
            [
                writer_pb2.Member(
                    id=n.id,
                    listen_address=n.address,
                    is_self=False,
                    dummy=n.dummy,
                    shard_count=n.shard_count,
                )
                for n in get_index_nodes()
            ]
        )
        return response

    async def GetResourceId(  # type: ignore
        self, request: ResourceIdRequest, context=None
    ) -> ResourceIdResponse:
        response = ResourceIdResponse()
        response.uuid = ""
        async with self.driver.transaction() as txn:
            kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
            rid = await kbobj.get_resource_uuid_by_slug(request.slug)
            if rid:
                response.uuid = rid
            return response

    async def ResourceFieldExists(  # type: ignore
        self, request: ResourceFieldId, context=None
    ) -> ResourceFieldExistsResponse:
        response = ResourceFieldExistsResponse()
        response.found = False
        resobj = None
        async with self.driver.transaction() as txn:
            kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
            resobj = ResourceORM(txn, self.storage, kbobj, request.rid)

            if request.field != "":
                field = await resobj.get_field(
                    request.field, request.field_type, load=True
                )
                if field.value is not None:
                    response.found = True
                else:
                    response.found = False
                return response

            if request.rid != "":
                if await resobj.exists():
                    response.found = True
                else:
                    response.found = False
                return response

            if request.kbid != "":
                config = await KnowledgeBoxORM.get_kb(txn, request.kbid)
                if config is not None:
                    response.found = True
                else:
                    response.found = False
                return response

            return response

    async def Index(self, request: IndexResource, context=None) -> IndexStatus:  # type: ignore
        async with self.driver.transaction() as txn:
            kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
            resobj = ResourceORM(txn, self.storage, kbobj, request.rid)
            bm = await resobj.generate_broker_message()
            transaction = get_transaction_utility()
            partitioning = get_partitioning()
            partition = partitioning.generate_partition(request.kbid, request.rid)
            await transaction.commit(bm, partition)

            response = IndexStatus()
            return response

    async def ReIndex(self, request: IndexResource, context=None) -> IndexStatus:  # type: ignore
        try:
            async with self.driver.transaction() as txn:
                kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
                resobj = ResourceORM(txn, self.storage, kbobj, request.rid)
                resobj.disable_vectors = not request.reindex_vectors

                brain = await resobj.generate_index_message()
                shard_id = await kbobj.get_resource_shard_id(request.rid)
                shard: Optional[writer_pb2.ShardObject] = None
                if shard_id is not None:
                    shard = await kbobj.get_resource_shard(shard_id)

                if shard is None:
                    shard = await self.shards_manager.get_current_active_shard(
                        txn, request.kbid
                    )
                    if shard is None:
                        # no shard currently exists, create one
                        model = await kbobj.get_model_metadata()
                        shard = await self.shards_manager.create_shard_by_kbid(
                            txn, request.kbid, semantic_model=model
                        )

                    await kbobj.set_resource_shard_id(request.rid, shard.shard)

                if shard is not None:
                    await self.shards_manager.add_resource(
                        shard,
                        brain.brain,
                        0,
                        partition=self.partitions[0],
                        kb=request.kbid,
                        reindex_id=uuid.uuid4().hex,
                    )

                response = IndexStatus()
                return response
        except Exception as e:
            errors.capture_exception(e)
            logger.error("Error in ingest gRPC servicer", exc_info=True)
            raise

    async def Export(self, request: ExportRequest, context=None):
        try:
            async with self.driver.transaction() as txn:
                kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
                async for resource in kbobj.iterate_resources():
                    yield await resource.generate_broker_message()
        except Exception:
            logger.exception("Export", stack_info=True)
            raise

    async def DownloadFile(self, request: FileRequest, context=None):
        async for data in self.storage.download(request.bucket, request.key):
            yield BinaryData(data=data)

    async def UploadFile(self, request: AsyncIterator[UploadBinaryData], context=None) -> FileUploaded:  # type: ignore
        data: UploadBinaryData

        destination: Optional[StorageField] = None
        cf = CloudFile()
        data = await request.__anext__()
        if data.HasField("metadata"):
            bucket = self.storage.get_bucket_name(data.metadata.kbid)
            destination = self.storage.field_klass(
                storage=self.storage, bucket=bucket, fullkey=data.metadata.key
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
        await self.storage.uploaditerator(
            generate_buffer(self.storage, request), destination, cf
        )
        result = FileUploaded()
        return result


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


def parse_model_metadata(request: KnowledgeBoxNew) -> SemanticModelMetadata:
    model = SemanticModelMetadata()
    model.similarity_function = request.similarity
    # TODO: remove `HasField` conditions once we are sure
    # they are always provided (both in self-hosted and cloud)
    if request.HasField("vector_dimension"):
        model.vector_dimension = request.vector_dimension
    if request.HasField("default_min_score"):
        model.default_min_score = request.default_min_score
    return model
