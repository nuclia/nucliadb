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
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID
from nucliadb_protos.knowledgebox_pb2 import Label as LabelPB
from nucliadb_protos.knowledgebox_pb2 import LabelSet as LabelSetPB
from nucliadb_protos.writer_pb2 import (
    DelEntitiesRequest,
    DelLabelsRequest,
    DelVectorSetRequest,
    NewEntitiesGroupRequest,
    NewEntitiesGroupResponse,
    OpStatusWriter,
    SetLabelsRequest,
    SetSynonymsRequest,
    UpdateEntitiesGroupRequest,
    UpdateEntitiesGroupResponse,
)
from starlette.requests import Request

from nucliadb.models.responses import (
    HTTPConflict,
    HTTPInternalServerError,
    HTTPNotFound,
)
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb.writer.resource.vectors import create_vectorset  # type: ignore
from nucliadb_models.entities import (
    CreateEntitiesGroupPayload,
    UpdateEntitiesGroupPayload,
)
from nucliadb_models.labels import LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_models.vectors import VectorSet
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroups",
    status_code=200,
    name="Create Knowledge Box Entities Group",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def create_entities_group(
    request: Request, kbid: str, item: CreateEntitiesGroupPayload
):
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
        return HTTPInternalServerError(
            detail="Error on settings entities on a Knowledge box"
        )


@api.patch(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    name="Update Knowledge Box Entities Group",
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
        return HTTPInternalServerError(
            detail="Error on settings entities on a Knowledge box"
        )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    name="Delete Knowledge Box Entities",
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
        raise HTTPException(
            status_code=500, detail="Error on deleting entities from a Knowledge box"
        )

    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    name="Set Knowledge Box Labels",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_labels(request: Request, kbid: str, labelset: str, item: LabelSet):
    ingest = get_ingest()
    pbrequest: SetLabelsRequest = SetLabelsRequest(id=labelset)
    pbrequest.kb.uuid = kbid

    if item.title:
        pbrequest.labelset.title = item.title

    if item.color:
        pbrequest.labelset.color = item.color

    pbrequest.labelset.multiple = item.multiple
    for kind in item.kind:
        pbrequest.labelset.kind.append(LabelSetPB.LabelSetKind.Value(kind))

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
        pbrequest.labelset.labels.append(lbl)
    status: OpStatusWriter = await ingest.SetLabels(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on settings labels on a Knowledge box"
        )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    name="Delete Knowledge Box Label",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_labels(request: Request, kbid: str, labelset: str):
    ingest = get_ingest()
    pbrequest: DelLabelsRequest = DelLabelsRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.id = labelset
    status: OpStatusWriter = await ingest.DelLabels(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on deleting labels from a Knowledge box"
        )
    return Response(status_code=204)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/vectorset/{{vectorset}}",
    status_code=200,
    name="Set Knowledge Box VectorSet",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_vectorset(request: Request, kbid: str, vectorset: str, item: VectorSet):
    await create_vectorset(kbid, vectorset, item.dimension, similarity=item.similarity)


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/vectorset/{{vectorset}}",
    status_code=200,
    name="Delete Knowledge Box VectorSet",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_vectorset(request: Request, kbid: str, vectorset: str):
    ingest = get_ingest()
    pbrequest: DelVectorSetRequest = DelVectorSetRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.vectorset = vectorset
    status: OpStatusWriter = await ingest.DelVectorSet(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on deleting vectorset from a Knowledge box"
        )
    return Response(status_code=204)


@api.put(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=204,
    name="Set Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_custom_synonyms(request: Request, kbid: str, item: KnowledgeBoxSynonyms):
    ingest = get_ingest()
    pbrequest = SetSynonymsRequest()
    pbrequest.kbid.uuid = kbid
    pbrequest.synonyms.CopyFrom(item.to_message())
    status: OpStatusWriter = await ingest.SetSynonyms(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return Response(status_code=204)
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error setting synonyms of a Knowledge box"
        )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=204,
    name="Delete Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_custom_synonyms(request: Request, kbid: str):
    ingest = get_ingest()
    pbrequest = KnowledgeBoxID(uuid=kbid)
    status: OpStatusWriter = await ingest.DelSynonyms(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return Response(status_code=204)
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error deleting synonyms of a Knowledge box"
        )
