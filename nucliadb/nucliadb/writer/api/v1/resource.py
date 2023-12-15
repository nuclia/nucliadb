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
import contextlib
from time import time
from typing import Optional
from uuid import uuid4

from fastapi import HTTPException, Query, Response
from fastapi_versioning import version
from grpc import StatusCode as GrpcStatusCode
from grpc.aio import AioRpcError  # type: ignore
from nucliadb_protos.resources_pb2 import Metadata
from nucliadb_protos.writer_pb2 import (
    BrokerMessage,
    IndexResource,
    ResourceFieldExistsResponse,
    ResourceFieldId,
    ResourceIdRequest,
    ResourceIdResponse,
)
from starlette.requests import Request

from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.processing import ProcessingInfo, PushPayload, Source
from nucliadb.writer import SERVICE_NAME, logger
from nucliadb.writer.api.constants import SKIP_STORE_DEFAULT, SYNC_CALL, X_NUCLIADB_USER
from nucliadb.writer.api.v1.router import (
    KB_PREFIX,
    RESOURCE_PREFIX,
    RESOURCES_PREFIX,
    RSLUG_PREFIX,
    api,
)
from nucliadb.writer.exceptions import IngestNotAvailable
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.basic import (
    parse_basic,
    parse_basic_modify,
    set_processing_info,
    set_status,
    set_status_modify,
)
from nucliadb.writer.resource.field import extract_fields, parse_fields
from nucliadb.writer.resource.origin import parse_extra, parse_origin
from nucliadb.writer.resource.slug import resource_slug_exists
from nucliadb.writer.resource.vectors import (
    create_vectorset,
    get_vectorsets,
    parse_vectors,
)
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceUpdated,
    UpdateResourcePayload,
)
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.utilities import (
    get_ingest,
    get_partitioning,
    get_storage,
    get_transaction_utility,
)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCES_PREFIX}",
    status_code=201,
    name="Create Resource",
    description="Create a new Resource in a Knowledge Box",
    response_model=ResourceCreated,
    response_model_exclude_unset=True,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def create_resource(
    request: Request,
    item: CreateResourcePayload,
    kbid: str,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
):
    transaction = get_transaction_utility()
    partitioning = get_partitioning()

    # Create resource message
    uuid = uuid4().hex
    partition = partitioning.generate_partition(kbid, uuid)

    writer = BrokerMessage()
    toprocess = PushPayload(
        uuid=uuid,
        kbid=kbid,
        partition=partition,
        userid=request.headers.get("X-NUCLIADB-USER", ""),
        processing_options=item.processing_options,
    )

    writer.kbid = kbid
    toprocess.kbid = kbid
    writer.uuid = uuid
    toprocess.uuid = uuid
    toprocess.source = Source.HTTP

    if item.slug:
        if await resource_slug_exists(kbid, item.slug):
            raise HTTPException(
                status_code=409, detail=f"Resource slug {item.slug} already exists"
            )
        writer.slug = item.slug
        toprocess.slug = item.slug

    parse_audit(writer.audit, request)
    parse_basic(writer, item, toprocess)

    if item.origin is not None:
        parse_origin(writer.origin, item.origin)
    if item.extra is not None:
        parse_extra(writer.extra, item.extra)

    await parse_fields(
        writer=writer,
        item=item,
        toprocess=toprocess,
        kbid=kbid,
        uuid=uuid,
        x_skip_store=x_skip_store,
    )

    if item.uservectors:
        vectorsets = await get_vectorsets(kbid)
        if vectorsets and len(vectorsets.vectorsets):
            parse_vectors(writer, item.uservectors, vectorsets)
        else:
            for vector in item.uservectors:
                if vector.vectors is not None:
                    for vectorset, uservector in vector.vectors.items():
                        if len(uservector) == 0:
                            raise HTTPException(
                                status_code=412,
                                detail=str("Vectorset without vector not allowed"),
                            )
                        first_vector = list(uservector.values())[0]
                        await create_vectorset(
                            kbid, vectorset, len(first_vector.vector)
                        )
            vectorsets = await get_vectorsets(kbid)
            if vectorsets is None or len(vectorsets.vectorsets) == 0:
                raise HTTPException(
                    status_code=412,
                    detail=str("Vectorset was not able to be created"),
                )
            parse_vectors(writer, item.uservectors, vectorsets)

    set_status(writer.basic, item)

    seqid = await maybe_send_to_process(writer, toprocess, partition)

    writer.source = BrokerMessage.MessageSource.WRITER
    if x_synchronous:
        t0 = time()
    await transaction.commit(writer, partition, wait=x_synchronous)

    if x_synchronous:
        return ResourceCreated(seqid=seqid, uuid=uuid, elapsed=time() - t0)
    else:
        return ResourceCreated(seqid=seqid, uuid=uuid)


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=200,
    name="Modify Resource (by slug)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def modify_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    item: UpdateResourcePayload,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
    x_nucliadb_user: str = X_NUCLIADB_USER,
):
    return await modify_resource_endpoint(
        request,
        item,
        kbid,
        path_rslug=rslug,
        x_skip_store=x_skip_store,
        x_synchronous=x_synchronous,
        x_nucliadb_user=x_nucliadb_user,
    )


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=200,
    name="Modify Resource (by id)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def modify_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    item: UpdateResourcePayload,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
    x_nucliadb_user: str = X_NUCLIADB_USER,
):
    return await modify_resource_endpoint(
        request,
        item,
        kbid,
        path_rid=rid,
        x_skip_store=x_skip_store,
        x_synchronous=x_synchronous,
        x_nucliadb_user=x_nucliadb_user,
    )


async def modify_resource_endpoint(
    request: Request,
    item: UpdateResourcePayload,
    kbid: str,
    x_skip_store: bool,
    x_synchronous: bool,
    x_nucliadb_user: str,
    path_rid: Optional[str] = None,
    path_rslug: Optional[str] = None,
):
    resource_uuid = await get_rid_from_params_or_raise_error(kbid, path_rid, path_rslug)
    if item.slug is None:
        return await modify_resource(
            request,
            item,
            kbid,
            x_skip_store=x_skip_store,
            x_synchronous=x_synchronous,
            x_nucliadb_user=x_nucliadb_user,
            rid=resource_uuid,
        )

    async with safe_update_resource_slug(
        request, kbid, rid=resource_uuid, new_slug=item.slug
    ):
        return await modify_resource(
            request,
            item,
            kbid,
            x_skip_store=x_skip_store,
            x_synchronous=x_synchronous,
            x_nucliadb_user=x_nucliadb_user,
            rid=resource_uuid,
        )


async def modify_resource(
    request: Request,
    item: UpdateResourcePayload,
    kbid: str,
    x_skip_store: bool,
    x_synchronous: bool,
    x_nucliadb_user: str,
    *,
    rid: str,
):
    transaction = get_transaction_utility()
    partitioning = get_partitioning()

    partition = partitioning.generate_partition(kbid, rid)

    writer = BrokerMessage()
    toprocess = PushPayload(
        uuid=rid,
        kbid=kbid,
        partition=partition,
        userid=x_nucliadb_user,
        processing_options=item.processing_options,
    )

    writer.kbid = kbid
    writer.uuid = rid
    toprocess.kbid = kbid
    toprocess.uuid = rid
    toprocess.source = Source.HTTP

    parse_basic_modify(writer, item, toprocess)
    parse_audit(writer.audit, request)
    if item.origin is not None:
        parse_origin(writer.origin, item.origin)
    if item.extra is not None:
        parse_extra(writer.extra, item.extra)

    await parse_fields(
        writer=writer,
        item=item,
        toprocess=toprocess,
        kbid=kbid,
        uuid=rid,
        x_skip_store=x_skip_store,
    )
    if item.uservectors:
        vectorsets = await get_vectorsets(kbid)
        if vectorsets:
            parse_vectors(writer, item.uservectors, vectorsets)
        else:
            raise HTTPException(status_code=412, detail=str("No vectorsets found"))

    set_status_modify(writer.basic, item)

    seqid = await maybe_send_to_process(writer, toprocess, partition)

    writer.source = BrokerMessage.MessageSource.WRITER

    maybe_mark_reindex(writer, item)

    await transaction.commit(writer, partition, wait=x_synchronous)

    return ResourceUpdated(seqid=seqid)


@contextlib.asynccontextmanager
async def safe_update_resource_slug(
    request,
    kbid: str,
    *,
    rid: str,
    new_slug: str,
):
    driver = get_app_context(request.app).kv_driver
    try:
        old_slug = await update_resource_slug(driver, kbid, rid=rid, new_slug=new_slug)
    except NotFoundError:
        raise HTTPException(status_code=404, detail=f"Resource does not exist: {rid}")
    except ConflictError:
        raise HTTPException(status_code=409, detail=f"Slug already exists: {new_slug}")
    try:
        yield
    except Exception:
        # Rollback slug update if something goes wrong
        try:
            await update_resource_slug(driver, kbid, rid=rid, new_slug=old_slug)
        except Exception as ex:
            capture_exception(ex)
            logger.exception("Error while rolling back slug update")
        raise


async def update_resource_slug(
    driver: Driver,
    kbid: str,
    *,
    rid: str,
    new_slug: str,
):
    async with driver.transaction() as txn:
        old_slug = await ResourcesDataManager.modify_slug(
            txn, kbid, rid=rid, new_slug=new_slug
        )
        await txn.commit()
        return old_slug


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/reprocess",
    status_code=202,
    name="Reprocess resource (by slug)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    x_nucliadb_user: str = X_NUCLIADB_USER,
):
    return await _reprocess_resource(kbid, rslug=rslug, x_nucliadb_user=x_nucliadb_user)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/reprocess",
    status_code=202,
    name="Reprocess resource (by id)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    x_nucliadb_user: str = X_NUCLIADB_USER,
):
    return await _reprocess_resource(kbid, rid=rid, x_nucliadb_user=x_nucliadb_user)


async def _reprocess_resource(
    kbid: str,
    x_nucliadb_user: str,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
):
    transaction = get_transaction_utility()
    partitioning = get_partitioning()

    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    partition = partitioning.generate_partition(kbid, rid)

    toprocess = PushPayload(
        uuid=rid,
        kbid=kbid,
        partition=partition,
        userid=x_nucliadb_user,
    )

    toprocess.kbid = kbid
    toprocess.uuid = rid
    toprocess.source = Source.HTTP

    storage = await get_storage(service_name=SERVICE_NAME)
    driver = get_driver()

    txn = await driver.begin()
    kb = KnowledgeBox(txn, storage, kbid)

    resource = await kb.get(rid)
    if resource is None:
        raise HTTPException(status_code=404, detail="Resource does not exist")

    await extract_fields(resource=resource, toprocess=toprocess)

    if txn.open:
        await txn.abort()

    processing_info = await send_to_process(toprocess, partition)

    writer = BrokerMessage()
    writer.kbid = kbid
    writer.uuid = rid
    writer.source = BrokerMessage.MessageSource.WRITER
    writer.basic.metadata.useful = True
    writer.basic.metadata.status = Metadata.Status.PENDING
    set_processing_info(writer, processing_info)
    await transaction.commit(writer, partition, wait=False)

    return ResourceUpdated(seqid=processing_info.seqid)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=204,
    name="Delete Resource (by slug)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    x_synchronous: bool = SYNC_CALL,
):
    return await _delete_resource(
        request, kbid, rslug=rslug, x_synchronous=x_synchronous
    )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=204,
    name="Delete Resource (by id)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    x_synchronous: bool = SYNC_CALL,
):
    return await _delete_resource(request, kbid, rid=rid, x_synchronous=x_synchronous)


async def _delete_resource(
    request: Request,
    kbid: str,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
):
    transaction = get_transaction_utility()
    partitioning = get_partitioning()

    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    partition = partitioning.generate_partition(kbid, rid)
    writer = BrokerMessage()

    writer.kbid = kbid
    writer.uuid = rid
    writer.type = BrokerMessage.MessageType.DELETE

    parse_audit(writer.audit, request)

    # Create processing message
    await transaction.commit(writer, partition, wait=x_synchronous)

    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/reindex",
    status_code=204,
    name="Reindex Resource (by slug)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reindex_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    reindex_vectors: bool = Query(False),
):
    return await _reindex_resource(kbid, rslug=rslug, reindex_vectors=reindex_vectors)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/reindex",
    status_code=204,
    name="Reindex Resource (by id)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reindex_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    reindex_vectors: bool = Query(False),
):
    return await _reindex_resource(kbid, rid=rid, reindex_vectors=reindex_vectors)


async def _reindex_resource(
    kbid: str,
    reindex_vectors: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
):
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    ingest = get_ingest()
    index_req = IndexResource()
    index_req.kbid = kbid
    index_req.rid = rid
    index_req.reindex_vectors = reindex_vectors

    await ingest.ReIndex(index_req)  # type: ignore
    return Response(status_code=200)


async def get_resource_uuid_from_slug(kbid: str, slug: str) -> str:
    ingest = get_ingest()
    pbrequest = ResourceIdRequest()
    pbrequest.kbid = kbid
    pbrequest.slug = slug
    try:
        response: ResourceIdResponse = await ingest.GetResourceId(pbrequest)  # type: ignore
    except AioRpcError as exc:
        if exc.code() is GrpcStatusCode.UNAVAILABLE:
            raise IngestNotAvailable()
        else:
            raise exc
    return response.uuid


async def get_rid_from_params_or_raise_error(
    kbid: str,
    rid: Optional[str] = None,
    slug: Optional[str] = None,
) -> str:
    if rid is not None:
        ingest = get_ingest()
        pbrequest = ResourceFieldId()
        pbrequest.kbid = kbid
        pbrequest.rid = rid

        try:
            response: ResourceFieldExistsResponse = await ingest.ResourceFieldExists(pbrequest)  # type: ignore
        except AioRpcError as exc:
            if exc.code() is GrpcStatusCode.UNAVAILABLE:
                raise IngestNotAvailable()
            else:
                raise exc

        if response.found:
            return rid
        else:
            raise HTTPException(status_code=404, detail="Resource does not exist")

    if slug is None:
        raise ValueError("Either rid or slug must be set")

    rid = await get_resource_uuid_from_slug(kbid, slug)
    if not rid:
        raise HTTPException(status_code=404, detail="Resource does not exist")

    return rid


def maybe_mark_reindex(message: BrokerMessage, item: UpdateResourcePayload):
    if needs_resource_reindex(item):
        message.reindex = True


def needs_resource_reindex(item: UpdateResourcePayload) -> bool:
    # Some metadata need to be applied as tags to all fields of
    # a resource and that means this message should force reindexing everything.
    # XXX This is not ideal. Long term, we should handle it differently
    # so this is not required
    return item.usermetadata is not None or (
        item.origin is not None
        and (
            item.origin.created is not None
            or item.origin.modified is not None
            or item.origin.metadata is not None
        )
    )


async def maybe_send_to_process(
    writer: BrokerMessage, toprocess: PushPayload, partition
) -> Optional[int]:
    if not needs_reprocess(toprocess):
        return None

    processing_info = await send_to_process(toprocess, partition)
    set_processing_info(writer, processing_info)
    return processing_info.seqid


async def send_to_process(toprocess: PushPayload, partition) -> ProcessingInfo:
    try:
        processing = get_processing()
        processing_info = await processing.send_to_process(toprocess, partition)
        return processing_info
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")


def needs_reprocess(processing_payload: PushPayload) -> bool:
    """
    Processing only pays attention to when there are fields to process,
    so sometimes we can skip sending to processing, for instance if a
    resource/paragraph is being annotated.
    """
    for field in (
        "genericfield",
        "filefield",
        "linkfield",
        "textfield",
        "layoutfield",
        "conversationfield",
    ):
        if len(getattr(processing_payload, field)) > 0:
            return True
    return False
