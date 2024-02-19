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
import json

from fastapi import HTTPException, Response
from fastapi_versioning import version
from nucliadb_protos.knowledgebox_pb2 import (
    DeleteKnowledgeBoxResponse,
    KnowledgeBoxID,
    KnowledgeBoxNew,
    KnowledgeBoxResponseStatus,
    KnowledgeBoxUpdate,
    NewKnowledgeBoxResponse,
    UpdateKnowledgeBoxResponse,
)
from starlette.requests import Request

from nucliadb.writer.api.v1.router import KB_PREFIX, KBS_PREFIX, api
from nucliadb.writer.utilities import get_processing
from nucliadb_models.resource import (
    KnowledgeBoxConfig,
    KnowledgeBoxObj,
    KnowledgeBoxObjID,
    NucliaDBRoles,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest


@api.post(
    f"/{KBS_PREFIX}",
    status_code=201,
    name="Create Knowledge Box",
    response_model=KnowledgeBoxObj,
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def create_kb(request: Request, item: KnowledgeBoxConfig):
    ingest = get_ingest()
    requestpb = KnowledgeBoxNew()
    requestpb = parse_create_kb_request(item)
    kbobj: NewKnowledgeBoxResponse = await ingest.NewKnowledgeBox(requestpb)  # type: ignore
    if item.slug != "":
        slug = item.slug
    else:
        slug = kbobj.uuid  # type: ignore
    if kbobj.status == KnowledgeBoxResponseStatus.OK:
        return KnowledgeBoxObj(uuid=kbobj.uuid, slug=slug)
    elif kbobj.status == KnowledgeBoxResponseStatus.CONFLICT:
        raise HTTPException(status_code=419, detail="Knowledge box already exists")
    elif kbobj.status == KnowledgeBoxResponseStatus.ERROR:
        raise HTTPException(status_code=500, detail="Error on creating knowledge box")


def parse_create_kb_request(item: KnowledgeBoxConfig) -> KnowledgeBoxNew:
    requestpb = KnowledgeBoxNew()
    if item.slug:
        requestpb.slug = item.slug
    if item.title:
        requestpb.config.title = item.title
    if item.description:
        requestpb.config.description = item.description
    if item.release_channel:
        requestpb.release_channel = item.release_channel.to_pb()
    if item.learning_configuration:
        requestpb.learning_config = json.dumps(item.learning_configuration)
    return requestpb


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}",
    status_code=200,
    name="Update Knowledge Box",
    response_model=KnowledgeBoxObjID,
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def update_kb(request: Request, kbid: str, item: KnowledgeBoxConfig):
    ingest = get_ingest()
    pbrequest = KnowledgeBoxUpdate(uuid=kbid)
    if item.slug is not None:
        pbrequest.slug = item.slug
    if item.title:
        pbrequest.config.title = item.title
    if item.description:
        pbrequest.config.description = item.description
    kbobj: UpdateKnowledgeBoxResponse = await ingest.UpdateKnowledgeBox(pbrequest)  # type: ignore
    if kbobj.status == KnowledgeBoxResponseStatus.OK:
        return KnowledgeBoxObjID(uuid=kbobj.uuid)
    elif kbobj.status == KnowledgeBoxResponseStatus.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge box does not exist")
    elif kbobj.status == KnowledgeBoxResponseStatus.ERROR:
        raise HTTPException(status_code=500, detail="Error on creating knowledge box")


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}",
    status_code=200,
    name="Delete Knowledge Box",
    response_model=KnowledgeBoxObj,
    tags=["Knowledge Boxes"],
    openapi_extra={"x-hidden-operation": True},
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def delete_kb(request: Request, kbid: str):
    ingest = get_ingest()

    kbobj: DeleteKnowledgeBoxResponse = await ingest.DeleteKnowledgeBox(  # type: ignore
        KnowledgeBoxID(uuid=kbid)
    )
    if kbobj.status == KnowledgeBoxResponseStatus.OK:
        return KnowledgeBoxObj(uuid=kbid)
    elif kbobj.status == KnowledgeBoxResponseStatus.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exists")
    elif kbobj.status == KnowledgeBoxResponseStatus.ERROR:
        raise HTTPException(status_code=500, detail="Error on deleting knowledge box")

    processing = get_processing()
    asyncio.create_task(processing.delete_from_processing(kbid=kbid))

    return Response(status_code=204)
