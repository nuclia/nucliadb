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
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

from fastapi import HTTPException, Response
from fastapi_versioning import version  # type: ignore
from nucliadb_protos.resources_pb2 import FieldID, FieldType, Metadata
from nucliadb_protos.writer_pb2 import BrokerMessage
from starlette.requests import Request

import nucliadb_models as models
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.processing import PushPayload, Source
from nucliadb.writer import SERVICE_NAME
from nucliadb.writer.api.constants import (
    SKIP_STORE_DEFAULT,
    SYNC_CALL,
    X_FILE_PASSWORD,
    X_NUCLIADB_USER,
)
from nucliadb.writer.api.v1.resource import get_rid_from_params_or_raise_error
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb.writer.resource.audit import parse_audit
from nucliadb.writer.resource.basic import set_processing_info
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
from nucliadb_models.writer import ResourceFieldAdded, ResourceUpdated
from nucliadb_utils.authentication import requires
from nucliadb_utils.exceptions import LimitsExceededError, SendToProcessError
from nucliadb_utils.utilities import (
    get_partitioning,
    get_storage,
    get_transaction_utility,
)

if TYPE_CHECKING:  # pragma: no cover
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: Dict[models.FieldTypeName, FieldType.V]
else:
    FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP: Dict[models.FieldTypeName, int]


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

    parse_audit(writer.audit, request)
    return writer, toprocess, partition


async def finish_field_put(
    writer: BrokerMessage,
    toprocess: PushPayload,
    partition: int,
    wait_on_commit: bool,
) -> int:
    # Create processing message
    transaction = get_transaction_utility()
    processing = get_processing()

    processing_info = await processing.send_to_process(toprocess, partition)

    writer.source = BrokerMessage.MessageSource.WRITER
    set_processing_info(writer, processing_info)
    await transaction.commit(writer, partition, wait=wait_on_commit)

    return processing_info.seqid


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/text/{{field_id}}",
    status_code=201,
    name="Add resource text field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_text_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.TextField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_text(
        request,
        kbid,
        field_id,
        field_payload,
        x_synchronous,
        rslug=rslug,
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/text/{{field_id}}",
    status_code=201,
    name="Add resource text field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_text_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.TextField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_text(
        request,
        kbid,
        field_id,
        field_payload,
        x_synchronous,
        rid=rid,
    )


async def _add_resource_field_text(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.TextField,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_text_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/link/{{field_id}}",
    status_code=201,
    name="Add resource link field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_link_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.LinkField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_link(
        request, kbid, field_id, field_payload, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/link/{{field_id}}",
    status_code=201,
    name="Add resource link field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_link_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.LinkField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_link(
        request, kbid, field_id, field_payload, x_synchronous, rid=rid
    )


async def _add_resource_field_link(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.LinkField,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_link_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/keywordset/{{field_id}}",
    status_code=201,
    name="Add resource keywordset field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_keywordset_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.FieldKeywordset,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_keywordset(
        request, kbid, field_id, field_payload, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/keywordset/{{field_id}}",
    status_code=201,
    name="Add resource keywordset field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_keywordset_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FieldKeywordset,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_keywordset(
        request, kbid, field_id, field_payload, x_synchronous, rid=rid
    )


async def _add_resource_field_keywordset(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.FieldKeywordset,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_keywordset_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/datetime/{{field_id}}",
    status_code=201,
    name="Add resource datetime field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_datetime_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.FieldDatetime,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_datetime(
        request, kbid, field_id, field_payload, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/datetime/{{field_id}}",
    status_code=201,
    name="Add resource datetime field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_datetime_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FieldDatetime,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_datetime(
        request, kbid, field_id, field_payload, x_synchronous, rid=rid
    )


async def _add_resource_field_datetime(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.FieldDatetime,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    parse_datetime_field(field_id, field_payload, writer, toprocess)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/layout/{{field_id}}",
    status_code=201,
    name="Add resource layout field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_layout_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.InputLayoutField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_layout(
        request,
        kbid,
        field_id,
        field_payload,
        x_synchronous,
        rslug=rslug,
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}",
    status_code=201,
    name="Add resource layout field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_layout_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.InputLayoutField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_layout(
        request, kbid, field_id, field_payload, x_synchronous, rid=rid
    )


async def _add_resource_field_layout(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.InputLayoutField,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    await parse_layout_field(field_id, field_payload, writer, toprocess, kbid, rid)
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}",
    status_code=201,
    name="Add resource conversation field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_conversation_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.InputConversationField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_conversation(
        request, kbid, field_id, field_payload, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}",
    status_code=201,
    name="Add resource conversation field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_conversation_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.InputConversationField,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_conversation(
        request, kbid, field_id, field_payload, x_synchronous, rid=rid
    )


async def _add_resource_field_conversation(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.InputConversationField,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    await parse_conversation_field(
        field_id, field_payload, writer, toprocess, kbid, rid
    )
    try:
        seqid = await finish_field_put(writer, toprocess, partition, x_synchronous)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/file/{{field_id}}",
    status_code=201,
    name="Add resource file field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    field_payload: models.FileField,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_file(
        request,
        kbid,
        field_id,
        field_payload,
        x_skip_store,
        x_synchronous,
        rslug=rslug,
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}",
    status_code=201,
    name="Add resource file field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def add_resource_field_file_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    field_payload: models.FileField,
    x_skip_store: bool = SKIP_STORE_DEFAULT,
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _add_resource_field_file(
        request, kbid, field_id, field_payload, x_skip_store, x_synchronous, rid=rid
    )


async def _add_resource_field_file(
    request: Request,
    kbid: str,
    field_id: str,
    field_payload: models.FileField,
    x_skip_store: bool,
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

    writer, toprocess, partition = prepare_field_put(kbid, rid, request)
    await parse_file_field(
        field_id, field_payload, writer, toprocess, kbid, rid, skip_store=x_skip_store
    )

    try:
        seqid = await finish_field_put(
            writer, toprocess, partition, wait_on_commit=x_synchronous
        )
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    return ResourceFieldAdded(seqid=seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}/messages",
    status_code=200,
    name="Append messages to conversation field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_messages_to_conversation_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    messages: List[models.InputMessage],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _append_messages_to_conversation_field(
        request, kbid, field_id, messages, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/messages",
    status_code=200,
    name="Append messages to conversation field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_messages_to_conversation_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    messages: List[models.InputMessage],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _append_messages_to_conversation_field(
        request, kbid, field_id, messages, x_synchronous, rid=rid
    )


async def _append_messages_to_conversation_field(
    request: Request,
    kbid: str,
    field_id: str,
    messages: List[models.InputMessage],
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    transaction = get_transaction_utility()
    processing = get_processing()
    partitioning = get_partitioning()

    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

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
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    writer.source = BrokerMessage.MessageSource.WRITER
    set_processing_info(writer, processing_info)
    await transaction.commit(writer, partition, wait=x_synchronous)

    return ResourceFieldAdded(seqid=processing_info.seqid)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/layout/{{field_id}}/blocks",
    status_code=200,
    name="Append blocks to layout field (by slug)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_blocks_to_layout_field_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: str,
    blocks: Dict[str, models.InputBlock],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _append_blocks_to_layout_field(
        request, kbid, field_id, blocks, x_synchronous, rslug=rslug
    )


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/layout/{{field_id}}/blocks",
    status_code=200,
    name="Append blocks to layout field (by id)",
    response_model=ResourceFieldAdded,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def append_blocks_to_layout_field_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    blocks: Dict[str, models.InputBlock],
    x_synchronous: bool = SYNC_CALL,
) -> ResourceFieldAdded:
    return await _append_blocks_to_layout_field(
        request, kbid, field_id, blocks, x_synchronous, rid=rid
    )


async def _append_blocks_to_layout_field(
    request: Request,
    kbid: str,
    field_id: str,
    blocks: Dict[str, models.InputBlock],
    x_synchronous: bool,
    rid: Optional[str] = None,
    rslug: Optional[str] = None,
) -> ResourceFieldAdded:
    transaction = get_transaction_utility()
    processing = get_processing()
    partitioning = get_partitioning()

    rid = await get_rid_from_params_or_raise_error(kbid, rid, rslug)

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
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    writer.source = BrokerMessage.MessageSource.WRITER
    set_processing_info(writer, processing_info)
    await transaction.commit(writer, partition, wait=x_synchronous)

    return ResourceFieldAdded(seqid=processing_info.seqid)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/{{field_type}}/{{field_id}}",
    status_code=204,
    name="Delete Resource field (by slug)",
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
    field_id: str,
    x_synchronous: bool = SYNC_CALL,
):
    return await _delete_resource_field(
        request,
        kbid,
        field_type,
        field_id,
        x_synchronous,
        rslug=rslug,
    )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/{{field_type}}/{{field_id}}",
    status_code=204,
    name="Delete Resource field (by id)",
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
    field_id: str,
    x_synchronous: bool = SYNC_CALL,
):
    return await _delete_resource_field(
        request, kbid, field_type, field_id, x_synchronous, rid=rid
    )


async def _delete_resource_field(
    request: Request,
    kbid: str,
    field_type: models.FieldTypeName,
    field_id: str,
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

    pb_field_id = FieldID()
    pb_field_id.field_type = FIELD_TYPE_NAME_TO_FIELD_TYPE_MAP[field_type]
    pb_field_id.field = field_id

    writer.delete_fields.append(pb_field_id)
    parse_audit(writer.audit, request)

    await transaction.commit(writer, partition, wait=x_synchronous)

    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/file/{{field_id}}/reprocess",
    status_code=202,
    name="Reprocess file field (by id)",
    response_model=models.writer.ResourceUpdated,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def reprocess_file_field(
    request: Request,
    kbid: str,
    rid: str,
    field_id: str,
    x_nucliadb_user: str = X_NUCLIADB_USER,
    x_file_password: Optional[str] = X_FILE_PASSWORD,
) -> ResourceUpdated:
    transaction = get_transaction_utility()
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

        try:
            await extract_file_field(
                field_id,
                resource=resource,
                toprocess=toprocess,
                password=x_file_password,
            )
        except KeyError:
            raise HTTPException(status_code=404, detail="Field does not exist")

    # Send current resource to reprocess.
    try:
        processing_info = await processing.send_to_process(toprocess, partition)
    except LimitsExceededError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail)
    except SendToProcessError:
        raise HTTPException(status_code=500, detail="Error while sending to process")

    writer = BrokerMessage()
    writer.kbid = kbid
    writer.uuid = rid
    writer.source = BrokerMessage.MessageSource.WRITER
    writer.basic.metadata.useful = True
    writer.basic.metadata.status = Metadata.Status.PENDING
    set_processing_info(writer, processing_info)
    await transaction.commit(writer, partition, wait=False)

    return ResourceUpdated(seqid=processing_info.seqid)
