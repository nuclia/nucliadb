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
from typing import AsyncIterator, Optional

from nucliadb.common import datamanagers
from nucliadb.common.cluster.exceptions import AlreadyExists, EntitiesGroupNotFound
from nucliadb.common.cluster.manager import get_index_nodes
from nucliadb.common.cluster.utils import get_shard_manager
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.external_index_providers.exceptions import ExternalIndexCreationError
from nucliadb.common.maindb.utils import setup_driver
from nucliadb.ingest import SERVICE_NAME, logger
from nucliadb.ingest.orm.broker_message import generate_broker_message
from nucliadb.ingest.orm.entities import EntitiesManager
from nucliadb.ingest.orm.exceptions import KnowledgeBoxConflict, VectorSetConflict
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox as KnowledgeBoxORM
from nucliadb.ingest.orm.processor import Processor, sequence_manager
from nucliadb.ingest.orm.resource import Resource as ResourceORM
from nucliadb.ingest.settings import settings
from nucliadb_protos import knowledgebox_pb2, nodewriter_pb2, writer_pb2, writer_pb2_grpc
from nucliadb_protos.knowledgebox_pb2 import (
    DeleteKnowledgeBoxResponse,
    KnowledgeBoxID,
    KnowledgeBoxNew,
    KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate,
    SemanticModelMetadata,
    UpdateKnowledgeBoxResponse,
    VectorSetConfig,
)
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    DelEntitiesRequest,
    DelVectorSetRequest,
    DelVectorSetResponse,
    GetEntitiesGroupRequest,
    GetEntitiesGroupResponse,
    GetEntitiesRequest,
    GetEntitiesResponse,
    IndexResource,
    IndexStatus,
    ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse,
    ListMembersRequest,
    ListMembersResponse,
    NewEntitiesGroupRequest,
    NewEntitiesGroupResponse,
    NewVectorSetRequest,
    NewVectorSetResponse,
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

    async def NewKnowledgeBox(  # type: ignore
        self, request: KnowledgeBoxNew, context=None
    ) -> knowledgebox_pb2.NewKnowledgeBoxResponse:
        if is_onprem_nucliadb():
            logger.error(
                "Sorry, this endpoint is only available for hosted. Onprem must use the REST API"
            )
            return knowledgebox_pb2.NewKnowledgeBoxResponse(
                status=KnowledgeBoxResponseStatus.ERROR,
            )

        # Hosted KBs are created through backend endpoints. We assume learning
        # configuration has been already created for it and we are given the
        # model metadata in the request

        kbid = request.forceuuid or KnowledgeBoxORM.new_unique_kbid()
        release_channel = request.release_channel
        semantic_model = parse_model_metadata_from_request(request)

        try:
            (kbid, _) = await KnowledgeBoxORM.create(
                self.driver,
                kbid=kbid,
                slug=request.slug,
                title=request.config.title,
                description=request.config.description,
                semantic_model=semantic_model,
                release_channel=release_channel,
                external_index_provider=request.external_index_provider,
            )

        except KnowledgeBoxConflict:
            logger.info("KB already exists", extra={"slug": request.slug})
            return knowledgebox_pb2.NewKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.CONFLICT)

        except ExternalIndexCreationError as exc:
            logger.exception(
                "Error creating external index",
                extra={"slug": request.slug, "error": str(exc)},
            )
            return knowledgebox_pb2.NewKnowledgeBoxResponse(
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
            return knowledgebox_pb2.NewKnowledgeBoxResponse(status=KnowledgeBoxResponseStatus.ERROR)

        else:
            logger.info("KB created successfully", extra={"kbid": kbid})
            return knowledgebox_pb2.NewKnowledgeBoxResponse(
                status=KnowledgeBoxResponseStatus.OK, uuid=kbid
            )

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

            entities_manager = EntitiesManager(kbobj, txn, use_read_replica_nodes=True)
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
                    primary_id=n.primary_id or "",
                )
                for n in get_index_nodes(include_secondary=True)
            ]
        )
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

                brain = await resobj.generate_index_message(reindex=True)
                shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=request.kbid, rid=request.rid
                )
                shard: Optional[writer_pb2.ShardObject] = None
                if shard_id is not None:
                    shard = await kbobj.get_resource_shard(shard_id)

                if shard is None:
                    shard = await self.shards_manager.get_current_active_shard(txn, request.kbid)
                    if shard is None:
                        # no shard currently exists, create one
                        shard = await self.shards_manager.create_shard_by_kbid(txn, request.kbid)

                    await datamanagers.resources.set_resource_shard_id(
                        txn, kbid=request.kbid, rid=request.rid, shard=shard.shard
                    )

                if shard is not None:
                    index_message = brain.brain
                    await self.proc._maybe_external_index_add_resource(
                        request.kbid,
                        nodewriter_pb2.IndexMessageSource.PROCESSOR,
                        request.rid,
                        index_message,
                    )
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

    async def NewVectorSet(  # type: ignore
        self, request: NewVectorSetRequest, context=None
    ) -> NewVectorSetResponse:
        config = VectorSetConfig(
            vectorset_id=request.vectorset_id,
            vectorset_index_config=nodewriter_pb2.VectorIndexConfig(
                similarity=request.similarity,
                normalize_vectors=request.normalize_vectors,
                vector_type=request.vector_type,
                vector_dimension=request.vector_dimension,
            ),
            matryoshka_dimensions=request.matryoshka_dimensions,
        )
        response = NewVectorSetResponse()
        try:
            async with self.driver.transaction() as txn:
                kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
                await kbobj.create_vectorset(config)
                await txn.commit()
        except VectorSetConflict as exc:
            response.status = NewVectorSetResponse.Status.ERROR
            response.details = str(exc)
        except Exception as exc:
            errors.capture_exception(exc)
            logger.error("Error in ingest gRPC while creating a vectorset", exc_info=True)
            response.status = NewVectorSetResponse.Status.ERROR
            response.details = str(exc)
        else:
            response.status = NewVectorSetResponse.Status.OK
        return response

    async def DelVectorSet(  # type: ignore
        self, request: DelVectorSetRequest, context=None
    ) -> DelVectorSetResponse:
        response = DelVectorSetResponse()
        try:
            async with self.driver.transaction() as txn:
                kbobj = KnowledgeBoxORM(txn, self.storage, request.kbid)
                await kbobj.delete_vectorset(request.vectorset_id)
                await txn.commit()
        except Exception as exc:
            errors.capture_exception(exc)
            logger.error("Error in ingest gRPC while deleting a vectorset", exc_info=True)
            response.status = DelVectorSetResponse.Status.ERROR
            response.details = str(exc)
        else:
            response.status = DelVectorSetResponse.Status.OK
        return response


def parse_model_metadata_from_request(
    request: KnowledgeBoxNew,
) -> SemanticModelMetadata:
    model = SemanticModelMetadata()
    model.similarity_function = request.similarity
    if request.HasField("vector_dimension"):
        model.vector_dimension = request.vector_dimension
    else:
        logger.warning(
            "Vector dimension not set. Will be detected automatically on the first vector set."
        )

    if len(request.matryoshka_dimensions) > 0:
        if model.vector_dimension not in request.matryoshka_dimensions:
            logger.warning(
                "Vector dimensions is inconsistent with matryoshka dimensions! Ignoring them",
                extra={
                    "kbid": request.forceuuid,
                    "kbslug": request.slug,
                },
            )
        else:
            model.matryoshka_dimensions.extend(request.matryoshka_dimensions)

    return model
