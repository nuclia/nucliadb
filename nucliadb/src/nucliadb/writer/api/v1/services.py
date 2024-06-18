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
from nucliadb.models.responses import (
    HTTPConflict,
    HTTPInternalServerError,
    HTTPNotFound,
)
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    UpdateEntitiesGroupPayload,
)
from nucliadb_models.labels import LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_protos import writer_pb2
from nucliadb_protos.knowledgebox_pb2 import Label as LabelPB
from nucliadb_protos.knowledgebox_pb2 import LabelSet as LabelSetPB
from nucliadb_protos.writer_pb2 import (
    DelEntitiesRequest,
    NewEntitiesGroupRequest,
    NewEntitiesGroupResponse,
    OpStatusWriter,
    UpdateEntitiesGroupRequest,
    UpdateEntitiesGroupResponse,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroups",
    status_code=200,
    summary="Create Knowledge Box Entities Group",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def create_entities_group(request: Request, kbid: str, item: CreateEntitiesGroupPayload):
    ingest = get_ingest()

    pbrequest: NewEntitiesGroupRequest = NewEntitiesGroupRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.group = item.group
    pbrequest.entities.custom = True
    if item.title:
        pbrequest.entities.title = item.title
    if item.color:
        pbrequest.entities.color = item.color

    for key, entity in item.entities.items():
        entitypb = pbrequest.entities.entities[key]
        entitypb.value = entity.value
        entitypb.merged = entity.merged
        entitypb.deleted = False
        entitypb.represents.extend(entity.represents)

    status: NewEntitiesGroupResponse = await ingest.NewEntitiesGroup(pbrequest)  # type: ignore
    if status.status == NewEntitiesGroupResponse.Status.OK:
        return
    elif status.status == NewEntitiesGroupResponse.Status.KB_NOT_FOUND:
        return HTTPNotFound(detail="Knowledge Box does not exist")
    elif status.status == NewEntitiesGroupResponse.Status.ALREADY_EXISTS:
        return HTTPConflict(
            detail=f"Entities group {item.group} already exists in this Knowledge box",
        )
    elif status.status == NewEntitiesGroupResponse.Status.ERROR:
        return HTTPInternalServerError(detail="Error on settings entities on a Knowledge box")


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    summary="Update Knowledge Box Entities Group",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 2},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def update_entities_group(
    request: Request, kbid: str, group: str, item: UpdateEntitiesGroupPayload
):
    ingest = get_ingest()

    pbrequest: UpdateEntitiesGroupRequest = UpdateEntitiesGroupRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.group = group
    pbrequest.title = item.title or ""
    pbrequest.color = item.color or ""

    for name, entity in item.add.items():
        entitypb = pbrequest.add[name]
        entitypb.value = entity.value
        entitypb.merged = entity.merged
        entitypb.represents.extend(entity.represents)

    for name, entity in item.update.items():
        entitypb = pbrequest.update[name]
        entitypb.value = entity.value
        entitypb.merged = entity.merged
        entitypb.represents.extend(entity.represents)

    pbrequest.delete.extend(item.delete)

    status: UpdateEntitiesGroupResponse = await ingest.UpdateEntitiesGroup(pbrequest)  # type: ignore
    if status.status == UpdateEntitiesGroupResponse.Status.OK:
        return
    elif status.status == UpdateEntitiesGroupResponse.Status.KB_NOT_FOUND:
        return HTTPNotFound(detail="Knowledge Box does not exist")
    elif status.status == UpdateEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND:
        return HTTPNotFound(detail="Entities group does not exist")
    elif status.status == UpdateEntitiesGroupResponse.Status.ERROR:
        return HTTPInternalServerError(detail="Error on settings entities on a Knowledge box")


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    summary="Delete Knowledge Box Entities",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_entities(request: Request, kbid: str, group: str):
    ingest = get_ingest()
    pbrequest: DelEntitiesRequest = DelEntitiesRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.group = group

    status: OpStatusWriter = await ingest.DelEntities(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(status_code=500, detail="Error on deleting entities from a Knowledge box")

    return Response(status_code=204)


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
    try:
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
    synonyms = item.to_message()
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
