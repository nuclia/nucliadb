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
from typing import Annotated, cast

import pydantic
from fastapi import HTTPException, Path, Response
from fastapi_versioning import version
from starlette.requests import Request

import nucliadb_models as models
from nucliadb.common import datamanagers
from nucliadb.ingest.fields.conversation import Conversation
from nucliadb.ingest.orm.knowledgebox import Resource as ORMResource
from nucliadb.writer import transaction
from nucliadb.writer.api.v1.field import (
    add_field_to_resource,
    add_field_to_resource_by_slug,
    validate_field_exists_or_raise_error,
)
from nucliadb.writer.api.v1.resource import (
    get_rid_from_slug_or_raise_error,
    validate_rid_exists_or_raise_error,
)
from nucliadb.writer.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RSLUG_PREFIX, api
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.utils import FieldIdString
from nucliadb_models.writer import ResourceFieldAdded
from nucliadb_protos import resources_pb2
from nucliadb_protos.resources_pb2 import FieldID
from nucliadb_protos.writer_pb2 import BrokerMessage, DeleteSplits
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import (
    get_partitioning,
)


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
    return await add_field_to_resource_by_slug(
        request, kbid, rslug, field_id, field_payload, replace_field=True
    )


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
    return await add_field_to_resource(request, kbid, rid, field_id, field_payload, replace_field=True)


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
    try:
        field = models.InputConversationField(messages=messages)
    except pydantic.ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return await add_field_to_resource_by_slug(
        request, kbid, rslug, field_id, field, replace_field=False
    )


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
    try:
        field = models.InputConversationField(messages=messages)
    except pydantic.ValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return await add_field_to_resource(request, kbid, rid, field_id, field, replace_field=False)


DELETE_CONVERSATION_MESSAGES_DESCRIPTION = (
    "Delete a message from a conversation field by its ident. "
    "This is a convenience endpoint intended for occasional deletions — "
    "frequent use on the same conversation may degrade read performance over time. "
    "Deleted message identifiers are permanently reserved and cannot be reused."
)


MessageIdent = Annotated[str, Path(max_length=128, description="The ident of the message to delete")]


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RSLUG_PREFIX}/{{rslug}}/conversation/{{field_id}}/messages/{{message_ident}}",
    status_code=204,
    summary="Delete conversation message (by slug)",
    description=DELETE_CONVERSATION_MESSAGES_DESCRIPTION,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_conversation_messages_rslug_prefix(
    request: Request,
    kbid: str,
    rslug: str,
    field_id: FieldIdString,
    message_ident: MessageIdent,
) -> Response:
    rid = await get_rid_from_slug_or_raise_error(kbid, rslug)
    return await _delete_conversation_messages(kbid, rid, field_id, [message_ident])


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/conversation/{{field_id}}/messages/{{message_ident}}",
    status_code=204,
    summary="Delete conversation message (by id)",
    description=DELETE_CONVERSATION_MESSAGES_DESCRIPTION,
    tags=["Resource fields"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_conversation_messages_rid_prefix(
    request: Request,
    kbid: str,
    rid: str,
    field_id: FieldIdString,
    message_ident: MessageIdent,
) -> Response:
    return await _delete_conversation_messages(kbid, rid, field_id, [message_ident])


async def validate_message_idents(kbid: str, rid: str, field_id: str, idents: list[str]) -> list[str]:
    """
    Takes the user provided idents to delete and checks agains the conversation metadata to return
    only the valid idents that can be deleted (i.e. that exist and haven't been previously deleted).
    This is to avoid sending to process invalid delete messages that would cause unnecessary load.
    """
    async with datamanagers.with_ro_transaction() as txn:
        resource_obj = await ORMResource.get(txn, kbid=kbid, rid=rid)
        if resource_obj is None:
            # Resource not found, nothing to delete
            return []
        field = await resource_obj.get_field(field_id, resources_pb2.FieldType.CONVERSATION, load=False)
        conv = cast(Conversation, field)
        splits_metadata = await conv.get_splits_metadata()
        previously_deleted_idents = set(splits_metadata.deleted_splits)
        valid_idents = set(splits_metadata.metadata.keys()) - previously_deleted_idents
        return list(set(idents) & valid_idents)


async def _delete_conversation_messages(
    kbid: str,
    rid: str,
    field_id: str,
    message_idents: list[str],
) -> Response:
    await validate_rid_exists_or_raise_error(kbid, rid)
    await validate_field_exists_or_raise_error(kbid, rid, field_id, resources_pb2.FieldType.CONVERSATION)
    to_delete = await validate_message_idents(kbid, rid, field_id, message_idents)
    if len(to_delete) == 0:
        # No valid message idents to delete, return early to avoid unnecessary load
        return Response(status_code=204)

    writer = BrokerMessage(kbid=kbid, uuid=rid, source=BrokerMessage.MessageSource.WRITER)
    delete_splits = DeleteSplits(
        field=FieldID(field=field_id, field_type=resources_pb2.FieldType.CONVERSATION),
        splits=to_delete,
    )
    writer.delete_splits.append(delete_splits)

    partitioning = get_partitioning()
    partition = partitioning.generate_partition(kbid, rid)
    await transaction.commit(writer, partition)
    return Response(status_code=204)
