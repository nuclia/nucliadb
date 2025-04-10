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
from typing import AsyncIterator

from nucliadb.backups import tasks as backup_tasks
from nucliadb.backups import utils as backup_utils
from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import AlreadyExists, EntitiesGroupNotFound
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.external_index_providers.manager import get_external_index_manager
from nucliadb.common.maindb.utils import setup_driver
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.broker_message import generate_broker_message
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict
from nucliadb.ingest.orm.index_message import get_resource_index_message
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.processor import Processor, sequence_manager
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.settings import settings
from nucliadb_protos import backups_pb2, writer_pb2, writer_pb2_grpc
from nucliadb_protos.knowledgebox_pb2 import (
    DeleteKnowledgeBoxResponse,
    KnowledgeBoxID,
    KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate,
    SemanticModelMetadata,
    UpdateKnowledgeBoxResponse,
)
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    DelEntitiesRequest,
    GetEntitiesGroupRequest,
    GetEntitiesGroupResponse,
    GetEntitiesRequest,
    GetEntitiesResponse,
    IndexResource,
    IndexStatus,
    ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse,
    NewEntitiesGroupRequest,
    NewEntitiesGroupResponse,
    OpStatusWriter,
    SetEntitiesRequest,
    UpdateEntitiesGroupRequest,
    UpdateEntitiesGroupResponse,
    WriterStatusRequest,
    WriterStatusResponse,
)
from nucliadb_telemetry import errors
from nucliadb_utils.settings import is_onprem_nucliadb
from nucliadb_utils.utilities import (
    get_partitioning,
    get_pubsub,
    get_storage,
    get_transaction_utility,
)


class WriterServicer(writer_pb2_grpc.WriterServicer):
    def __init__(self):
        self.partitions = settings.partitions

    async def initialize(self):
        self.storage = await get_storage(service_name=SERVICE_NAME)
        self.driver = await setup_driver()
        self.proc = Processor(driver=self.driver, storage=self.storage, pubsub=await get_pubsub())
        self.shards_manager = get_shard_manager()

    async def finalize(self): ...

    async def NewKnowledgeBoxV2(
        self, request: writer_pb2.NewKnowledgeBoxV2Request, context=None
    ) -> writer_pb2.NewKnowledgeBoxV2Response:
        """v2 of KB creation endpoint. Payload has been refactored and cleaned
        up to include only necessary fields. It has also been extended to
        support KB creation with multiple vectorsets
        """
        if is_onprem_nucliadb():
            logger.error(
                "Sorry, this endpoint is only available for hosted. Onprem must use the REST API"
            )
            return writer_pb2.NewKnowledgeBoxV2Response(
                status=KnowledgeBoxResponseStatus.ERROR,
                error_message="This endpoint is only available for hosted. Onprem must use the REST API",
            )
        # Hosted KBs are created through backend endpoints. We assume learning
        # configuration has been already created for it and we are given the
        # model metadata in the request

        try:
            kbid, _ = await KnowledgeBoxORM.create(
                self.driver,
                kbid=request.kbid,
                slug=request.slug,
                title=request.title,
                description=request.description,
                semantic_models={
                    vs.vectorset_id: SemanticModelMetadata(
                        similarity_function=vs.similarity,
                        vector_dimension=vs.vector_dimension,
                        matryoshka_dimensions=vs.matryoshka_dimensions,
                    )
                    for vs in request.vectorsets
                },
                external_index_provider=request.external_index_provider,
                hidden_resources_enabled=request.hidden_resources_enabled,
                hidden_resources_hide_on_creation=request.hidden_resources_hide_on_creation,
            )

        except KnowledgeBoxConflict:
            logger.info("KB already exists", extra={"slug": request.slug})
            return writer_pb2.NewKnowledgeBoxV2Response(status=KnowledgeBoxResponseStatus.CONFLICT)

        except ExternalIndexCreationError as exc:
            logger.exception(
                "Error creating external index",
                extra={"slug": request.slug, "error": str(exc)},
            )
            return writer_pb2.NewKnowledgeBoxV2Response(
                status=KnowledgeBoxResponseStatus.EXTERNAL_INDEX_PROVIDER_ERROR,
                error_message=exc.message,
            )

        except Exception as exc:
            errors.capture_exception(exc)
            logger.exception(
                "Unexpected error creating KB",
                exc_info=True,
                extra={"slug": request.slug},
            )
            return writer_pb2.NewKnowledgeBoxV2Response(status=KnowledgeBoxResponseStatus.ERROR)

        else:
            logger.info("KB created successfully", extra={"kbid": kbid})
            return writer_pb2.NewKnowledgeBoxV2Response(status=KnowledgeBoxResponseStatus.OK)

    async def UpdateKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxUpdate, context=None
    ) -> UpdateKnowledgeBoxResponse:
        if is_onprem_nucliadb():
            logger.error(
                "Sorry, this endpoint is only available for hosted. Onprem must use the REST API"
            )
            return UpdateKnowledgeBoxResponse(
                status=KnowledgeBoxResponseStatus.ERROR,
            )

        try:
            async with self.driver.transaction() as txn:
                kbid = await KnowledgeBoxORM.update(
                    txn, uuid=request.uuid, slug=request.slug, config=request.config
                )
                await txn.commit()
        except KnowledgeBoxNotFound:
            return UpdateKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.NOTFOUND)
        except Exception:
            logger.exception("Could not update KB", exc_info=True)
            return UpdateKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)
        return UpdateKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.OK, uuid=kbid)

    async def DeleteKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxID, context=None
    ) -> DeleteKnowledgeBoxResponse:
        if is_onprem_nucliadb():
            logger.error(
                "Sorry, this endpoint is only available for hosted. Onprem must use the REST API"
            )
            return DeleteKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)

        try:
            kbid = request.uuid
            # learning configuration is automatically removed in nuclia backend for
            # hosted users, we don't need to do it
            await KnowledgeBoxORM.delete(self.driver, kbid=kbid)
        except KnowledgeBoxNotFound:
            logger.warning(f"KB not found: kbid={request.uuid}, slug={request.slug}")
        except Exception:
            logger.exception("Could not delete KB", exc_info=True)
            return DeleteKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)
        return DeleteKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.OK)

    async def ProcessMessage(
        self, request_stream: AsyncIterator[BrokerMessage], context=None
    ) -> OpStatusWriter:
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
        return response

    async def NewEntitiesGroup(  # type: ignore
        self, request: NewEntitiesGroupRequest, context=None
    ) -> NewEntitiesGroupResponse:
        response = NewEntitiesGroupResponse()
        async with self.driver.transaction(read_only=True) as ro_txn:
            kbobj = await self.proc.get_kb_obj(ro_txn, request.kb)
            if kbobj is None:
                response.status = NewEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

        async with self.driver.transaction() as txn:
            kbobj.txn = txn
            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.create_entities_group(request.group, request.entities)
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
        async with self.driver.transaction(read_only=True) as txn:
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
        async with self.driver.transaction(read_only=True) as txn:
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
        async with self.driver.transaction(read_only=True) as txn:
            kbobj = await self.proc.get_kb_obj(txn, request.kb)
            if kbobj is None:
                response.status = GetEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

            entities_manager = EntitiesManager(kbobj, txn)
            try:
                entities_group = await entities_manager.get_entities_group(request.group)
            except Exception as e:
                errors.capture_exception(e)
                logger.error("Error in ingest gRPC servicer", exc_info=True)
                response.status = GetEntitiesGroupResponse.Status.ERROR
            else:
                response.kb.uuid = kbobj.kbid
                if entities_group is None:
                    response.status = GetEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND
                else:
                    response.status = GetEntitiesGroupResponse.Status.OK
                    response.group.CopyFrom(entities_group)

            return response

    async def SetEntities(self, request: SetEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        response = OpStatusWriter()
        async with self.driver.transaction(read_only=True) as ro_txn:
            kbobj = await self.proc.get_kb_obj(ro_txn, request.kb)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response

        async with self.driver.transaction() as txn:
            kbobj.txn = txn
            entities_manager = EntitiesManager(kbobj, txn)
            try:
                await entities_manager.set_entities_group(request.group, request.entities)
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
        async with self.driver.transaction(read_only=True) as ro_txn:
            kbobj = await self.proc.get_kb_obj(ro_txn, request.kb)
            if kbobj is None:
                response.status = UpdateEntitiesGroupResponse.Status.KB_NOT_FOUND
                return response

        async with self.driver.transaction() as txn:
            kbobj.txn = txn
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
                response.status = UpdateEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND
                return response

            await txn.commit()
            response.status = UpdateEntitiesGroupResponse.Status.OK
            return response

    async def DelEntities(self, request: DelEntitiesRequest, context=None) -> OpStatusWriter:  # type: ignore
        response = OpStatusWriter()

        async with self.driver.transaction(read_only=True) as ro_txn:
            kbobj = await self.proc.get_kb_obj(ro_txn, request.kb)
            if kbobj is None:
                response.status = OpStatusWriter.Status.NOTFOUND
                return response

        async with self.driver.transaction() as txn:
            kbobj.txn = txn
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

    async def Status(  # type: ignore
        self, request: WriterStatusRequest, context=None
    ) -> WriterStatusResponse:
        logger.info("Status Call")
        response = WriterStatusResponse()
        async with self.driver.transaction(read_only=True) as txn:
            async for _, slug in datamanagers.kb.get_kbs(txn):
                response.knowledgeboxes.append(slug)

            for partition in settings.partitions:
                seq_id = await sequence_manager.get_last_seqid(self.driver, partition)
                if seq_id is not None:
                    response.msgid[partition] = seq_id

            return response

    async def Index(self, request: IndexResource, context=None) -> IndexStatus:  # type: ignore
        async with self.driver.transaction() as txn:
            kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
            resobj = ResourceORM(txn, self.storage, kbobj, request.rid)
            bm = await generate_broker_message(resobj)
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
                index_message = await get_resource_index_message(resobj, reindex=True)
                shard = await self.proc.get_or_assign_resource_shard(txn, kbobj, request.rid)
                external_index_manager = await get_external_index_manager(kbid=request.kbid)
                if external_index_manager is not None:
                    await self.proc.external_index_add_resource(
                        request.kbid,
                        request.rid,
                        index_message,
                    )
                else:
                    await self.shards_manager.add_resource(
                        shard,
                        index_message,
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

    async def CreateBackup(
        self, request: backups_pb2.CreateBackupRequest, context=None
    ) -> backups_pb2.CreateBackupResponse:
        if not await exists_kb(request.kb_id):
            return backups_pb2.CreateBackupResponse(
                status=backups_pb2.CreateBackupResponse.Status.KB_NOT_FOUND
            )
        await backup_tasks.create(request.kb_id, request.backup_id)
        return backups_pb2.CreateBackupResponse(status=backups_pb2.CreateBackupResponse.Status.OK)

    async def DeleteBackup(
        self, request: backups_pb2.DeleteBackupRequest, context=None
    ) -> backups_pb2.DeleteBackupResponse:
        if not await backup_utils.exists_backup(self.storage, request.backup_id):
            return backups_pb2.DeleteBackupResponse(
                status=backups_pb2.DeleteBackupResponse.Status.OK,
            )
        await backup_tasks.delete(request.backup_id)
        return backups_pb2.DeleteBackupResponse(status=backups_pb2.DeleteBackupResponse.Status.OK)

    async def RestoreBackup(
        self, request: backups_pb2.RestoreBackupRequest, context=None
    ) -> backups_pb2.RestoreBackupResponse:
        if not await exists_kb(request.kb_id):
            return backups_pb2.RestoreBackupResponse(
                status=backups_pb2.RestoreBackupResponse.Status.NOT_FOUND
            )
        if not await backup_utils.exists_backup(self.storage, request.backup_id):
            return backups_pb2.RestoreBackupResponse(
                status=backups_pb2.RestoreBackupResponse.Status.NOT_FOUND
            )
        await backup_tasks.restore(request.kb_id, request.backup_id)
        return backups_pb2.RestoreBackupResponse(status=backups_pb2.RestoreBackupResponse.Status.OK)


async def exists_kb(kbid: str) -> bool:
    return await datamanagers.atomic.kb.exists_kb(kbid=kbid)
