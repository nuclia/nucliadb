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
from starlette.requests import Request

from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.utils import get_driver
from nucliadb.reader.api.v1.router import KB_PREFIX, KBS_PREFIX, api
from nucliadb_models.resource import (
    KnowledgeBoxList,
    KnowledgeBoxObj,
    KnowledgeBoxObjSummary,
    NucliaDBRoles,
)
from nucliadb_utils.authentication import requires, requires_one


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
    driver = await get_driver()
    txn = await driver.begin()
    response = KnowledgeBoxList()
    async for kbid, slug in KnowledgeBox.get_kbs(txn, prefix):
        response.kbs.append(KnowledgeBoxObjSummary(slug=slug or None, uuid=kbid))
    await txn.abort()
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
    driver = await get_driver()
    txn = await driver.begin()
    kb_config = await KnowledgeBox.get_kb(txn, kbid)
    await txn.abort()

    if kb_config is None:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

    return KnowledgeBoxObj(
        uuid=kbid,
        slug=kb_config.slug,
        config=MessageToDict(kb_config, preserving_proto_field_name=True),
    )


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
    driver = await get_driver()
    txn = await driver.begin()

    kbid = await KnowledgeBox.get_kb_uuid(txn, slug)
    if kbid is None:
        await txn.abort()
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

    kb_config = await KnowledgeBox.get_kb(txn, kbid)
    if kb_config is None:
        await txn.abort()
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

    await txn.abort()
    return KnowledgeBoxObj(
        uuid=kbid,
        slug=kb_config.slug,
        config=MessageToDict(kb_config, preserving_proto_field_name=True),
    )
