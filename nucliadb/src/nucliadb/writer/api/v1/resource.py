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
import contextlib
from time import time
from typing import Annotated, Optional
from uuid import uuid4

from fastapi import HTTPException, Query, Response
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.common.back_pressure import maybe_back_pressure
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.maindb.driver import Driver
from nucliadb.common.maindb.exceptions import ConflictError, NotFoundError
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.models.internal.processing import ProcessingInfo, PushPayload, Source
from nucliadb.writer import SERVICE_NAME, logger
from nucliadb.writer.api.constants import X_NUCLIADB_USER, X_SKIP_STORE
from nucliadb.writer.api.v1 import transaction
from nucliadb.writer.api.v1.router import (
    KB_PREFIX,
    RESOURCE_PREFIX,
    RESOURCES_PREFIX,
    RSLUG_PREFIX,
    api,
)
from nucliadb.writer.api.v1.slug import ensure_slug_uniqueness, noop_context_manager
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.basic import (
    parse_basic_creation,
    parse_basic_modify,
    parse_user_classifications,
    set_status,
    set_status_modify,
)
from nucliadb.writer.resource.field import (
    ResourceClassifications,
    atomic_get_stored_resource_classifications,
    extract_fields,
    parse_fields,
)
from nucliadb.writer.resource.origin import parse_extra, parse_origin
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.writer import (
    CreateResourcePayload,
    ResourceCreated,
    ResourceUpdated,
    UpdateResourcePayload,
)
from nucliadb_protos.resources_pb2 import FieldID, Metadata
from nucliadb_protos.writer_pb2 import BrokerMessage, FieldIDStatus, FieldStatus, IndexResource
from nucliadb_telemetry.errors import capture_exception
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.utilities import (
    get_ingest,
    get_partitioning,
    get_storage,
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
    x_skip_store: Annotated[bool, X_SKIP_STORE] = False,
    x_nucliadb_user: Annotated[str, X_NUCLIADB_USER] = "",
):
    kb_config = await datamanagers.atomic.kb.get_config(kbid=kbid)
    if item.hidden and not (kb_config and kb_config.hidden_resources_enabled):
        raise HTTPException(
            status_code=422,
            detail="Cannot hide a resource: the KB does not have hidden resources enabled",
        )

    await maybe_back_pressure(kbid)

    partitioning = get_partitioning()

    # Create resource message
    uuid = uuid4().hex
    partition = partitioning.generate_partition(kbid, uuid)

    writer = BrokerMessage()
    toprocess = PushPayload(
        uuid=uuid,
        kbid=kbid,
        partition=partition,
        userid=x_nucliadb_user,
        processing_options=item.processing_options,
    )

    writer.kbid = kbid
    toprocess.kbid = kbid
    writer.uuid = uuid
    toprocess.uuid = uuid
    toprocess.source = Source.HTTP
    toprocess.title = item.title

    unique_slug_context_manager = noop_context_manager()
    if item.slug:
        unique_slug_context_manager = ensure_slug_uniqueness(kbid, item.slug)
        writer.slug = item.slug
        toprocess.slug = item.slug

    async with unique_slug_context_manager:
        parse_audit(writer.audit, request)
        parse_basic_creation(writer, item, toprocess, kb_config)

        if item.origin is not None:
            parse_origin(writer.origin, item.origin)
        if item.extra is not None:
            parse_extra(writer.extra, item.extra)

        # Since this is a resource creation, we need to care only about the user-provided
        # classifications in the request.
        resource_classifications = ResourceClassifications(
            resource_level=set(parse_user_classifications(item))
        )
        await parse_fields(
            writer=writer,
            item=item,
            toprocess=toprocess,
            kbid=kbid,
            uuid=uuid,
            x_skip_store=x_skip_store,
            resource_classifications=resource_classifications,
        )

        set_status(writer.basic, item)

        writer.source = BrokerMessage.MessageSource.WRITER

        if item.wait_for_commit:
            t0 = time()
        await transaction.commit(writer, partition, wait=item.wait_for_commit)

        if item.wait_for_commit:
            txn_time = time() - t0
        else:
            txn_time = None

        seqid = await maybe_send_to_process(toprocess, partition)

        return ResourceCreated(seqid=seqid, uuid=uuid, elapsed=txn_time)


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=200,
    summary="Modify Resource (by slug)",
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
    x_skip_store: Annotated[bool, X_SKIP_STORE] = False,
    x_nucliadb_user: Annotated[str, X_NUCLIADB_USER] = "",
):
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await modify_resource_endpoint(
        request,
        item,
        kbid,
        rid,
        x_skip_store=x_skip_store,
        x_nucliadb_user=x_nucliadb_user,
    )


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=200,
    summary="Modify Resource (by id)",
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
    x_nucliadb_user: Annotated[str, X_NUCLIADB_USER] = "",
    x_skip_store: Annotated[bool, X_SKIP_STORE] = False,
):
    return await modify_resource_endpoint(
        request,
        item,
        kbid,
        rid,
        x_skip_store=x_skip_store,
        x_nucliadb_user=x_nucliadb_user,
    )


async def modify_resource_endpoint(
    request: Request,
    item: UpdateResourcePayload,
    kbid: str,
    rid: str,
    x_skip_store: bool,
    x_nucliadb_user: str,
):
    await validate_rid_exists_or_raise_error(kbid, rid)

    await maybe_back_pressure(kbid, resource_uuid=rid)

    if item.slug is None:
        return await modify_resource(
            request,
            item,
            kbid,
            x_skip_store=x_skip_store,
            x_nucliadb_user=x_nucliadb_user,
            rid=rid,
        )

    async with safe_update_resource_slug(request, kbid, rid=rid, new_slug=item.slug):
        return await modify_resource(
            request,
            item,
            kbid,
            x_skip_store=x_skip_store,
            x_nucliadb_user=x_nucliadb_user,
            rid=rid,
        )


async def modify_resource(
    request: Request,
    item: UpdateResourcePayload,
    kbid: str,
    x_skip_store: bool,
    x_nucliadb_user: str,
    *,
    rid: str,
):
    kb_config = await datamanagers.atomic.kb.get_config(kbid=kbid)
    if item.hidden and not (kb_config and kb_config.hidden_resources_enabled):
        raise HTTPException(
            status_code=422,
            detail="Cannot hide a resource: the KB does not have hidden resources enabled",
        )

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

    if item.usermetadata is not None:
        # If usermetadata is set in the request payload, this means that stored resource classifications
        # are not valid and we need to use the ones provided by the user in the request
        resource_classifications = ResourceClassifications(
            resource_level=set(parse_user_classifications(item))
        )
    else:
        resource_classifications = await atomic_get_stored_resource_classifications(kbid, rid)

    await parse_fields(
        writer=writer,
        item=item,
        toprocess=toprocess,
        kbid=kbid,
        uuid=rid,
        x_skip_store=x_skip_store,
        resource_classifications=resource_classifications,
    )
    set_status_modify(writer.basic, item)

    toprocess.title = writer.basic.title

    writer.source = BrokerMessage.MessageSource.WRITER

    maybe_mark_reindex(writer, item)

    await transaction.commit(writer, partition)
    seqid = await maybe_send_to_process(toprocess, partition)

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
        old_slug = await datamanagers.resources.modify_slug(txn, kbid=kbid, rid=rid, new_slug=new_slug)
        await txn.commit()
        return old_slug


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/reprocess",
    status_code=202,
    summary="Reprocess resource (by slug)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    x_nucliadb_user: Annotated[str, X_NUCLIADB_USER] = "",
):
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await _reprocess_resource(request, kbid, rid, x_nucliadb_user=x_nucliadb_user)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/reprocess",
    status_code=202,
    summary="Reprocess resource (by id)",
    response_model=ResourceUpdated,
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    x_nucliadb_user: Annotated[str, X_NUCLIADB_USER] = "",
):
    return await _reprocess_resource(request, kbid, rid, x_nucliadb_user=x_nucliadb_user)


async def _reprocess_resource(
    request: Request,
    kbid: str,
    rid: str,
    x_nucliadb_user: str,
):
    await validate_rid_exists_or_raise_error(kbid, rid)
    await maybe_back_pressure(kbid, resource_uuid=rid)

    partitioning = get_partitioning()

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

    writer = BrokerMessage()
    async with driver.transaction() as txn:
        kb = KnowledgeBox(txn, storage, kbid)

        resource = await kb.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

        await extract_fields(resource=resource, toprocess=toprocess)
        for field_type, field_id in resource.fields.keys():
            writer.field_statuses.append(
                FieldIDStatus(
                    id=FieldID(field_type=field_type, field=field_id),
                    status=FieldStatus.Status.PENDING,
                )
            )

    writer.kbid = kbid
    writer.uuid = rid
    writer.source = BrokerMessage.MessageSource.WRITER
    writer.basic.metadata.useful = True
    writer.basic.metadata.status = Metadata.Status.PENDING
    await transaction.commit(writer, partition, wait=False)
    processing_info = await send_to_process(toprocess, partition)

    return ResourceUpdated(seqid=processing_info.seqid)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}",
    status_code=204,
    summary="Delete Resource (by slug)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
):
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await _delete_resource(request, kbid, rid)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}",
    status_code=204,
    summary="Delete Resource (by id)",
    tags=["Resources"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
):
    return await _delete_resource(request, kbid, rid)


async def _delete_resource(
    request: Request,
    kbid: str,
    rid: str,
):
    await validate_rid_exists_or_raise_error(kbid, rid)

    partitioning = get_partitioning()

    partition = partitioning.generate_partition(kbid, rid)
    writer = BrokerMessage()

    writer.kbid = kbid
    writer.uuid = rid
    writer.type = BrokerMessage.MessageType.DELETE

    parse_audit(writer.audit, request)
    await transaction.commit(writer, partition)
    processing = get_processing()
    asyncio.create_task(processing.delete_from_processing(kbid=kbid, resource_id=rid))

    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/reindex",
    status_code=204,
    summary="Reindex Resource (by slug)",
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
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await _reindex_resource(request, kbid, rid, reindex_vectors=reindex_vectors)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/reindex",
    status_code=204,
    summary="Reindex Resource (by id)",
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
    return await _reindex_resource(request, kbid, rid, reindex_vectors=reindex_vectors)


async def _reindex_resource(
    request: Request,
    kbid: str,
    rid: str,
    reindex_vectors: bool,
):
    await validate_rid_exists_or_raise_error(kbid, rid)
    await maybe_back_pressure(kbid, resource_uuid=rid)

    ingest = get_ingest()
    index_req = IndexResource()
    index_req.kbid = kbid
    index_req.rid = rid
    index_req.reindex_vectors = reindex_vectors

    await ingest.ReIndex(index_req)  # type: ignore
    return Response(status_code=200)


async def get_rid_from_slug_or_raise_error(kbid: str, rslug: str) -> str:
    rid = await datamanagers.atomic.resources.get_resource_uuid_from_slug(kbid=kbid, slug=rslug)
    if not rid:
        raise HTTPException(status_code=404, detail="Resource does not exist")
    return rid


async def validate_rid_exists_or_raise_error(kbid: str, rid: str):
    if not (await datamanagers.atomic.resources.resource_exists(kbid=kbid, rid=rid)):
        raise HTTPException(status_code=404, detail="Resource does not exist")


def maybe_mark_reindex(message: BrokerMessage, item: UpdateResourcePayload):
    if needs_resource_reindex(item):
        message.reindex = True


def needs_resource_reindex(item: UpdateResourcePayload) -> bool:
    # Some metadata need to be applied as tags to all fields of
    # a resource and that means this message should force reindexing everything.
    # XXX This is not ideal. Long term, we should handle it differently
    # so this is not required
    return (
        item.usermetadata is not None
        or item.hidden is not None
        or (
            item.origin is not None
            and (
                item.origin.created is not None
                or item.origin.modified is not None
                or item.origin.metadata is not None
            )
        )
        or item.security is not None
    )


async def maybe_send_to_process(toprocess: PushPayload, partition) -> Optional[int]:
    if not needs_reprocess(toprocess):
        return None

    processing_info = await send_to_process(toprocess, partition)
    return processing_info.seqid


async def send_to_process(toprocess: PushPayload, partition) -> ProcessingInfo:
    try:
        processing = get_processing()
        processing_info = await processing.send_to_process(toprocess, partition)
        return processing_info
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(
            status_code=500,
            detail="Error while sending to process. Try calling /reprocess",
        )


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
        "conversationfield",
    ):
        if len(getattr(processing_payload, field)) > 0:
            return True
    return False
