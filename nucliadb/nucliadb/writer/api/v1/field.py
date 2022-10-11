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
from typing import TYPE_CHECKING, Dict, List, Tuple

from fastapi import HTTPException, Response
from fastapi.params import Header
from fastapi_versioning import version  # type: ignore
from nucliadb_protos.resources_pb2 import FieldID, FieldType
from nucliadb_protos.writer_pb2 import BrokerMessage
from starlette.requests import Request

import nucliadb.models as models
from nucliadb.ingest.processing import PushPayload, Source
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.models.writer import ResourceFieldAdded
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, api
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.basic import set_last_seqid
from nucliadb.writer.resource.field import (
    parse_conversation_field,
    parse_datetime_field,
    parse_external_file_field,
    parse_file_field,
    parse_internal_file_field,
    parse_keywordset_field,
    parse_layout_field,
    parse_link_field,
    parse_text_field,
)
from nucliadb.writer.utilities import get_processing
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError
from nucliadb_utils.utilities import get_partitioning, get_transaction

if TYPE_CHECKING:
    SKIP_STORE_DEFAULT = False
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: Dict[models.FieldTypeName, FieldType.V]
    SYNC_CALL = False
else:
    SKIP_STORE_DEFAULT = Header(False)
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: Dict[models.FieldTypeName, int]
    SYNC_CALL = Header(False)


FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP = {
    models.FieldTypeName.FILE: FieldType.FILE,
    models.FieldTypeName.LINK: FieldType.LINK,
    models.FieldTypeName.DATETIME: FieldType.DATETIME,
    models.FieldTypeName.KEYWORDSET: FieldType.KEYWORDSET,
    models.FieldTypeName.TEXT: FieldType.TEXT,
    models.FieldTypeName.LAYOUT: FieldType.LAYOUT,
    # models.FieldTypeName.GENERIC: FieldType.GENERIC,
    models.FieldTypeName.CONVERSATION: FieldType.CONVERSATION,
}


def prepare_field_put(
    kbid: str, rid: str, request: Request
) -> Tuple[BrokerMessage, PushPayload, int]:

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

    set_info_on_span({"nuclia.rid": rid, "nuclia.kbid": kbid})

    parse_audit(writer.audit, request)
    return writer, toprocess, partition


async def finish_field_put(
    writer: BrokerMessage,
    toprocess: PushPayload,
    partition: int,
    wait_on_commit: bool,
) -> int:
    # Create processing message
    transaction = get_transaction()
    processing = get_processing()

    seqid = await processing.send_to_process(toprocess, partition)

    writer.source = BrokerMessage.MessageSource.WRITER
    set_last_seqid(writer, seqid)
    await transaction.commit(writer, partition, wait=wait_on_commit)

    return seqid


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/text/{{field_id}}",
    status_code=201,
    name="Add resource text field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_text(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.TextField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_text_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/link/{{field_id}}",
    status_code=201,
    name="Add resource link field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_link(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.LinkField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_link_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/keywordset/{{field_id}}",
    status_code=201,
    name="Add resource keywordset field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_keywordset(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FieldKeywordset,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_keywordset_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/datetime/{{field_id}}",
    status_code=201,
    name="Add resource datetime field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_datetime(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FieldDatetime,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_datetime_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}",
    status_code=201,
    name="Add resource layout field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_layout(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.InputLayoutField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    await parse_layout_field(field_id, field_payload, writer, toprocess, kbid, rid)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}",
    status_code=201,
    name="Add resource conversation field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_conversation(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.InputConversationField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    await parse_conversation_field(
        field_id, field_payload, writer, toprocess, kbid, rid
    )
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}",
    status_code=201,
    name="Add resource file field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_internal_or_external_file(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FileField,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:

    if field_payload.file.is_external:
        return await add_resource_field_file_external(
            request, kbid, rid, field_id, field_payload, x_synchronous
        )
    else:
        return await add_resource_field_file_internal(
            request, kbid, rid, field_id, field_payload, x_skip_store, x_synchronous
        )


async def add_resource_field_file_internal(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FileField,
    x_skip_store: bool,
    x_synchronous: bool,
) -> ResourceFieldAdded:
    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    if x_skip_store:
        await parse_file_field(field_id, field_payload, writer, toprocess)
    else:
        await parse_internal_file_field(
            field_id, field_payload, writer, toprocess, kbid, rid
        )

    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


async def add_resource_field_file_external(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FileField,
    wait_on_commit: bool,
) -> ResourceFieldAdded:
    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_external_file_field(field_id, field_payload, writer, toprocess)

    try:
        seqid = await finish_field_put(
            writer, toprocess, partition, wait_on_commit=wait_on_commit
        )
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/messages",
    status_code=200,
    name="Append messages to conversation field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_messages_to_conversation_field(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    messages: List[models.InputMessage],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    transaction = get_transaction()
    processing = get_processing()
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

    field = models.InputConversationField()
    field.messages.extend(messages)

    await parse_conversation_field(field_id, field, writer, toprocess, kbid, rid)

    try:
        seqid = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    writer.source = BrokerMessage.MessageSource.WRITER
    set_last_seqid(writer, seqid)
    await transaction.commit(writer, partition, wait=x_synchronous)

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}/blocks",
    status_code=200,
    name="Append blocks to layout field",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_blocks_to_layout_field(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    blocks: Dict[str, models.InputBlock],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    transaction = get_transaction()
    processing = get_processing()
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

    field = models.InputLayoutField(body=models.InputLayoutContent())
    field.body.blocks.update(blocks)
    await parse_layout_field(field_id, field, writer, toprocess, kbid, rid)

    try:
        seqid = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=402, detail=str(exc))

    writer.source = BrokerMessage.MessageSource.WRITER
    set_last_seqid(writer, seqid)
    await transaction.commit(writer, partition, wait=x_synchronous)

    return ResourceFieldAdded(seqid=seqid)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}",
    status_code=204,
    name="Delete Resource field",
    response_model_exclude_unset=True,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_resource_field(
    request: Request,
    rid: str,
    kbid: str,
    field_type: models.FieldTypeName,
    field_id: str,
    x_synchronous: bool = SYNC_CALL,
):
    transaction = get_transaction()
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

    await transaction.commit(writer, partition, wait=x_synchronous)

    return Response(status_code=204)
