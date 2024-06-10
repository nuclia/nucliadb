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
from inspect import iscoroutinefunction
from typing import TYPE_CHECKING, Callable, Optional, Type, Union

from fastapi import HTTPException, Response
from fastapi_versioning import version
from starlette.requests import Request

import nucliadb_models as models
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.processing import PushPayload, Source
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.api.constants import (
    SKIP_STORE_DEFAULT,
    X_FILE_PASSWORD,
    X_NUCLIADB_USER,
)
from nucliadb.writer.api.v1 import transaction
from nucliadb.writer.api.v1.resource import (
    get_rid_from_slug_or_raise_error,
    validate_rid_exists_or_raise_error,
)
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb.writer.back_pressure import maybe_back_pressure
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.field import (
    extract_file_field,
    parse_conversation_field,
    parse_datetime_field,
    parse_file_field,
    parse_keywordset_field,
    parse_layout_field,
    parse_link_field,
    parse_text_field,
)
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import ResourceFieldAdded, ResourceUpdated
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldID, Metadata
from nucliadb_protos.writer_pb2 import BrokerMessage
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.utilities import (
    get_partitioning,
    get_storage,
)

if TYPE_CHECKING:  # pragma: no cover
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: dict[models.FieldTypeName, resources_pb2.FieldType.V]
else:
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: dict[models.FieldTypeName, int]

FieldModelType = Union[
    models.TextField,
    models.LinkField,
    models.FieldKeywordset,
    models.FieldDatetime,
    models.InputLayoutField,
    models.InputConversationField,
    models.FileField,
]

FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP = {
    models.FieldTypeName.FILE: resources_pb2.FieldType.FILE,
    models.FieldTypeName.LINK: resources_pb2.FieldType.LINK,
    models.FieldTypeName.DATETIME: resources_pb2.FieldType.DATETIME,
    models.FieldTypeName.KEYWORDSET: resources_pb2.FieldType.KEYWORDSET,
    models.FieldTypeName.TEXT: resources_pb2.FieldType.TEXT,
    models.FieldTypeName.LAYOUT: resources_pb2.FieldType.LAYOUT,
    # models.FieldTypeName.GENERIC: resources_pb2.FieldType.GENERIC,
    models.FieldTypeName.CONVERSATION: resources_pb2.FieldType.CONVERSATION,
}


async def add_field_to_resource(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: FieldModelType,
    **parser_kwargs,
):
    await validate_rid_exists_or_raise_error(kbid, rid)
    await maybe_back_pressure(request, kbid, resource_uuid=rid)

    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)

    writer = BrokerMessage()
    toprocess = PushPayload(
        uuid=rid,
        kbid=kbid,
        partition=partition,
        userid=request.headers.get("X-NUCLIADB-USER", ""),
    )

    writer.kbid = kbid
    writer.uuid = rid
    toprocess.kbid = kbid
    toprocess.uuid = rid
    toprocess.source = Source.HTTP

    parse_audit(writer.audit, request)

    parse_field = FIELD_PARSERS_MAP[type(field_payload)]
    if iscoroutinefunction(parse_field):
        await parse_field(kbid, rid, field_id, field_payload, writer, toprocess, **parser_kwargs)
    else:
        parse_field(kbid, rid, field_id, field_payload, writer, toprocess, **parser_kwargs)

    processing = get_processing()
    await transaction.commit(writer, partition)
    try:
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(
            status_code=500,
            detail="Error while sending to process. Try calling /reprocess",
        )
    seqid = processing_info.seqid
    return ResourceFieldAdded(seqid=seqid)


async def add_field_to_resource_by_slug(
    request: Request,
    kbid: str,
    slug: str,
    field_id: FieldIdString,
    field_payload: FieldModelType,
    **parser_kwargs,
):
    rid = await get_rid_from_slug_or_raise_error(kbid, slug)
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload, **parser_kwargs)


async def delete_resource_field(
    request: Request,
    kbid: str,
    rid: str,
    field_type: models.FieldTypeName,
    field_id: FieldIdString,
):
    await validate_rid_exists_or_raise_error(kbid, rid)

    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)
    writer = BrokerMessage()

    writer.kbid = kbid
    writer.uuid = rid

    pb_field_id = FieldID()
    pb_field_id.field_type = FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP[field_type]
    pb_field_id.field = field_id

    writer.delete_fields.append(pb_field_id)
    parse_audit(writer.audit, request)
    await transaction.commit(writer, partition)
    return Response(status_code=204)


async def delete_resource_field_by_slug(
    request: Request,
    kbid: str,
    slug: str,
    field_type: models.FieldTypeName,
    field_id: FieldIdString,
):
    rid = await get_rid_from_slug_or_raise_error(kbid, slug)
    return await delete_resource_field(
        request,
        kbid,
        rid,
        field_type,
        field_id,
    )


# Adapters for each parse function


def parse_text_field_adapter(
    _kbid: str,
    _rid: str,
    field_id: FieldIdString,
    field_payload: models.TextField,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return parse_text_field(field_id, field_payload, writer, toprocess)


def parse_link_field_adapter(
    _kbid: str,
    _rid: str,
    field_id: FieldIdString,
    field_payload: models.LinkField,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return parse_link_field(field_id, field_payload, writer, toprocess)


def parse_keywordset_field_adapter(
    _kbid: str,
    _rid: str,
    field_id: FieldIdString,
    field_payload: models.FieldKeywordset,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return parse_keywordset_field(field_id, field_payload, writer, toprocess)


def parse_datetime_field_adapter(
    _kbid: str,
    _rid: str,
    field_id: FieldIdString,
    field_payload: models.FieldDatetime,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return parse_datetime_field(field_id, field_payload, writer, toprocess)


async def parse_layout_field_adapter(
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.InputLayoutField,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return await parse_layout_field(field_id, field_payload, writer, toprocess, kbid, rid)


async def parse_conversation_field_adapter(
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.InputConversationField,
    writer: BrokerMessage,
    toprocess: PushPayload,
):
    return await parse_conversation_field(field_id, field_payload, writer, toprocess, kbid, rid)


async def parse_file_field_adapter(
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.FileField,
    writer: BrokerMessage,
    toprocess: PushPayload,
    skip_store: bool,
):
    return await parse_file_field(
        field_id, field_payload, writer, toprocess, kbid, rid, skip_store=skip_store
    )


FIELD_PARSERS_MAP: dict[Type, Union[Callable]] = {
    models.TextField: parse_text_field_adapter,
    models.LinkField: parse_link_field_adapter,
    models.FieldKeywordset: parse_keywordset_field_adapter,
    models.FieldDatetime: parse_datetime_field_adapter,
    models.InputLayoutField: parse_layout_field_adapter,
    models.InputConversationField: parse_conversation_field_adapter,
    models.FileField: parse_file_field_adapter,
}


# API endpoints


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/text/{{field_id}}",
    status_code=201,
    summary="Add resource text field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_text_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.TextField,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/text/{{field_id}}",
    status_code=201,
    summary="Add resource text field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_text_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.TextField,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/link/{{field_id}}",
    status_code=201,
    summary="Add resource link field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_link_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.LinkField,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/link/{{field_id}}",
    status_code=201,
    summary="Add resource link field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_link_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.LinkField,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/keywordset/{{field_id}}",
    status_code=201,
    summary="Add resource keywordset field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_keywordset_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.FieldKeywordset,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/keywordset/{{field_id}}",
    status_code=201,
    summary="Add resource keywordset field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_keywordset_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.FieldKeywordset,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/datetime/{{field_id}}",
    status_code=201,
    summary="Add resource datetime field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_datetime_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.FieldDatetime,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/datetime/{{field_id}}",
    status_code=201,
    summary="Add resource datetime field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_datetime_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.FieldDatetime,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/layout/{{field_id}}",
    status_code=201,
    summary="Add resource layout field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_layout_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.InputLayoutField,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}",
    status_code=201,
    summary="Add resource layout field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_layout_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.InputLayoutField,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}",
    status_code=201,
    summary="Add resource conversation field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_conversation_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.InputConversationField,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}",
    status_code=201,
    summary="Add resource conversation field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_conversation_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.InputConversationField,
) -> ResourceFieldAdded:
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}",
    status_code=201,
    summary="Add resource file field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    field_payload: models.FileField,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
) -> ResourceFieldAdded:
    return await add_field_to_resource_by_slug(
        request, kbid, rslug, field_id, field_payload, skip_store=x_skip_store
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}",
    status_code=201,
    summary="Add resource file field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    field_payload: models.FileField,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
) -> ResourceFieldAdded:
    return await add_field_to_resource(
        request, kbid, rid, field_id, field_payload, skip_store=x_skip_store
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}/messages",
    status_code=200,
    summary="Append messages to conversation field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_messages_to_conversation_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    messages: list[models.InputMessage],
) -> ResourceFieldAdded:
    field = models.InputConversationField()
    field.messages.extend(messages)
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/messages",
    status_code=200,
    summary="Append messages to conversation field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_messages_to_conversation_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    messages: list[models.InputMessage],
) -> ResourceFieldAdded:
    field = models.InputConversationField()
    field.messages.extend(messages)
    return await add_field_to_resource(request, kbid, rid, field_id, field)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/layout/{{field_id}}/blocks",
    status_code=200,
    summary="Append blocks to layout field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_blocks_to_layout_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    blocks: dict[str, models.InputBlock],
) -> ResourceFieldAdded:
    field = models.InputLayoutField(body=models.InputLayoutContent())
    field.body.blocks.update(blocks)
    return await add_field_to_resource_by_slug(request, kbid, rslug, field_id, field)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}/blocks",
    status_code=200,
    summary="Append blocks to layout field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_blocks_to_layout_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    blocks: dict[str, models.InputBlock],
) -> ResourceFieldAdded:
    field = models.InputLayoutField(body=models.InputLayoutContent())
    field.body.blocks.update(blocks)
    return await add_field_to_resource(request, kbid, rid, field_id, field)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}",
    status_code=204,
    summary="Delete Resource field (by slug)",
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_type: models.FieldTypeName,
    field_id: FieldIdString,
):
    return await delete_resource_field_by_slug(request, kbid, rslug, field_type, field_id)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}",
    status_code=204,
    summary="Delete Resource field (by id)",
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_type: models.FieldTypeName,
    field_id: FieldIdString,
):
    return await delete_resource_field(request, kbid, rid, field_type, field_id)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}/reprocess",
    status_code=202,
    summary="Reprocess file field (by id)",
    response_model=models.writer.ResourceUpdated,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_file_field(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    x_nucliadb_user: str = X_NUCLIADB_USER,
    x_file_password: Optional[str] = X_FILE_PASSWORD,
) -> ResourceUpdated:
    await maybe_back_pressure(request, kbid, resource_uuid=rid)

    processing = get_processing()
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

    async with driver.transaction() as txn:
        kb = KnowledgeBox(txn, storage, kbid)

        resource = await kb.get(rid)
        if resource is None:
            raise HTTPException(status_code=404, detail="Resource does not exist")

        if resource.basic is not None:
            toprocess.title = resource.basic.title

        try:
            await extract_file_field(
                field_id,
                resource=resource,
                toprocess=toprocess,
                password=x_file_password,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="Field does not exist")

    writer = BrokerMessage()
    writer.kbid = kbid
    writer.uuid = rid
    writer.source = BrokerMessage.MessageSource.WRITER
    writer.basic.metadata.useful = True
    writer.basic.metadata.status = Metadata.Status.PENDING
    await transaction.commit(writer, partition, wait=False)
    # Send current resource to reprocess.
    try:
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(
            status_code=500,
            detail="Error while sending to process. Try calling /reprocess",
        )

    return ResourceUpdated(seqid=processing_info.seqid)
