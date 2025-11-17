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
from fastapi import HTTPException, Response
from fastapi_versioning import version
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.models_utils import to_proto
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.configuration import SearchConfiguration
from nucliadb_models.labels import LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_protos import writer_pb2
from nucliadb_protos.knowledgebox_pb2 import Label as LabelPB
from nucliadb_protos.knowledgebox_pb2 import LabelSet as LabelSetPB
from nucliadb_utils.authentication import requires


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    summary="Set Knowledge Box Labels",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_labelset_endpoint(request: Request, kbid: str, labelset: str, item: LabelSet):
    if item.title is None:
        item.title = labelset

    try:
        labelsets = await datamanagers.atomic.labelset.get_all(kbid=kbid)
        labelset_titles = {
            ls.title.lower(): k for (k, ls) in labelsets.labelset.items() if k != labelset
        }
        if item.title.lower() in labelset_titles:
            conflict = labelset_titles[item.title.lower()]
            raise HTTPException(
                status_code=422,
                detail=f"Duplicated labelset titles are not allowed. Labelset {conflict} has the same title",
            )

        await set_labelset(kbid, labelset, item)
    except KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")


async def set_labelset(kbid: str, labelset_id: str, item: LabelSet):
    kb_exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    if not kb_exists:
        raise KnowledgeBoxNotFound()
    labelset = writer_pb2.LabelSet()
    if item.title is not None:
        labelset.title = item.title
    if item.color is not None:
        labelset.color = item.color
    labelset.multiple = item.multiple
    for kind in item.kind:
        labelset.kind.append(LabelSetPB.LabelSetKind.Value(kind))
    for label_input in item.labels:
        lbl = LabelPB()
        if label_input.uri:
            lbl.uri = label_input.uri
        if label_input.text:
            lbl.text = label_input.text
        if label_input.related:
            lbl.related = label_input.related
        if label_input.title:
            lbl.title = label_input.title
        labelset.labels.append(lbl)
    await datamanagers.atomic.labelset.set(kbid=kbid, labelset_id=labelset_id, labelset=labelset)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    summary="Delete Knowledge Box Label",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_labelset_endpoint(request: Request, kbid: str, labelset: str):
    try:
        await delete_labelset(kbid, labelset)
    except KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")


async def delete_labelset(kbid: str, labelset_id: str):
    kb_exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    if not kb_exists:
        raise KnowledgeBoxNotFound()
    await datamanagers.atomic.labelset.delete(kbid=kbid, labelset_id=labelset_id)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=204,
    summary="Set Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_custom_synonyms(request: Request, kbid: str, item: KnowledgeBoxSynonyms):
    if not await datamanagers.atomic.kb.exists_kb(kbid=kbid):
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    synonyms = to_proto.kb_synonyms(item)
    await datamanagers.atomic.synonyms.set(kbid=kbid, synonyms=synonyms)
    return Response(status_code=204)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=204,
    summary="Delete Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_custom_synonyms(request: Request, kbid: str):
    async with datamanagers.with_transaction() as txn:
        if not await datamanagers.kb.exists_kb(txn, kbid=kbid):
            raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

        await datamanagers.synonyms.delete(txn, kbid=kbid)
        await txn.commit()

    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/search_configurations/{{config_name}}",
    status_code=201,
    summary="Create search configuration",
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def create_search_configuration(
    request: Request, kbid: str, config_name: str, search_configuration: SearchConfiguration
):
    async with datamanagers.with_transaction() as txn:
        if not await datamanagers.kb.exists_kb(txn, kbid=kbid):
            raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

        if await datamanagers.search_configurations.get(txn, kbid=kbid, name=config_name) is not None:
            raise HTTPException(status_code=409, detail="Search configuration already exists")

        await datamanagers.search_configurations.set(
            txn, kbid=kbid, name=config_name, config=search_configuration
        )
        await txn.commit()

    return Response(status_code=201)


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/search_configurations/{{config_name}}",
    status_code=200,
    summary="Update search configuration",
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def update_search_configuration(
    request: Request, kbid: str, config_name: str, search_configuration: SearchConfiguration
):
    async with datamanagers.with_transaction() as txn:
        if not await datamanagers.kb.exists_kb(txn, kbid=kbid):
            raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

        if await datamanagers.search_configurations.get(txn, kbid=kbid, name=config_name) is None:
            raise HTTPException(status_code=404, detail="Search configuration does not exist")

        await datamanagers.search_configurations.set(
            txn, kbid=kbid, name=config_name, config=search_configuration
        )
        await txn.commit()

    return Response(status_code=200)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/search_configurations/{{config_name}}",
    status_code=204,
    summary="Delete search configuration",
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_search_configuration(request: Request, kbid: str, config_name: str):
    async with datamanagers.with_transaction() as txn:
        if not await datamanagers.kb.exists_kb(txn, kbid=kbid):
            raise HTTPException(status_code=404, detail="Knowledge Box does not exist")

        if await datamanagers.search_configurations.get(txn, kbid=kbid, name=config_name) is None:
            raise HTTPException(status_code=404, detail="Search configuration does not exist")

        await datamanagers.search_configurations.delete(txn, kbid=kbid, name=config_name)
        await txn.commit()

    return Response(status_code=204)
