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
from fastapi import HTTPException
from fastapi_versioning import version  # type: ignore
from google.protobuf.json_format import MessageToDict
from nucliadb_protos.knowledgebox_pb2 import (
    KnowledgeBox,
    KnowledgeBoxID,
    KnowledgeBoxPrefix,
    KnowledgeBoxResponseStatus,
)
from starlette.requests import Request

from nucliadb.reader.api.v1.router import KB_PREFIX, KBS_PREFIX, api
from nucliadb_models.resource import (
    KnowledgeBoxList,
    KnowledgeBoxObj,
    KnowledgeBoxObjSummary,
    NucliaDBRoles,
)
from nucliadb_utils.authentication import requires, requires_one
from nucliadb_utils.utilities import get_ingest


@api.get(
    f"/{KBS_PREFIX}",
    status_code=200,
    name="List Knowledge Boxes",
    response_model=KnowledgeBoxList,
    tags=["Knowledge Boxes"],
    include_in_schema=False,
)
@requires(NucliaDBRoles.MANAGER)
@version(1)
async def get_kbs(request: Request, prefix: str = "") -> KnowledgeBoxList:
    ingest = get_ingest()
    response = KnowledgeBoxList()
    async for kb_id in ingest.ListKnowledgeBox(KnowledgeBoxPrefix(prefix=prefix)):  # type: ignore
        if kb_id.slug == "":
            slug = None
        else:
            slug = kb_id.slug
        response.kbs.append(KnowledgeBoxObjSummary(slug=slug, uuid=kb_id.uuid))
    return response


@api.get(
    f"/{KB_PREFIX}/{{kbid}}",
    status_code=200,
    name="Get Knowledge Box",
    response_model=KnowledgeBoxObj,
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def get_kb(request: Request, kbid: str) -> KnowledgeBoxObj:
    ingest = get_ingest()
    kbobj: KnowledgeBox = await ingest.GetKnowledgeBox(KnowledgeBoxID(uuid=kbid))  # type: ignore
    if kbobj.status == KnowledgeBoxResponseStatus.OK:
        return KnowledgeBoxObj(
            uuid=kbobj.uuid,
            slug=kbobj.slug,
            config=MessageToDict(kbobj.config, preserving_proto_field_name=True),
        )
    elif kbobj.status == KnowledgeBoxResponseStatus.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Uknonwn GRPC response")


@api.get(
    f"/{KB_PREFIX}/s/{{slug}}",
    status_code=200,
    name="Get Knowledge Box (by slug)",
    response_model=KnowledgeBoxObj,
    tags=["Knowledge Boxes"],
)
@requires_one([NucliaDBRoles.MANAGER, NucliaDBRoles.READER])
@version(1)
async def get_kb_by_slug(request: Request, slug: str) -> KnowledgeBoxObj:
    ingest = get_ingest()
    kbobj: KnowledgeBox = await ingest.GetKnowledgeBox(KnowledgeBoxID(slug=slug))  # type: ignore
    if kbobj.status == KnowledgeBoxResponseStatus.OK:
        return KnowledgeBoxObj(
            uuid=kbobj.uuid,
            slug=kbobj.slug,
            config=MessageToDict(kbobj.config, preserving_proto_field_name=True),
        )
    elif kbobj.status == KnowledgeBoxResponseStatus.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Uknonwn GRPC response")
