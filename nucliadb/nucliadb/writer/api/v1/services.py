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
from fastapi_versioning import version  # type: ignore
from nucliadb_protos.knowledgebox_pb2 import Label as LabelPB
from nucliadb_protos.knowledgebox_pb2 import LabelSet as LabelSetPB
from nucliadb_protos.knowledgebox_pb2 import Widget as WidgetPB
from nucliadb_protos.writer_pb2 import (
    DelEntitiesRequest,
    DelLabelsRequest,
    DetWidgetsRequest,
    OpStatusWriter,
    SetEntitiesRequest,
    SetLabelsRequest,
    SetWidgetsRequest,
)
from starlette.requests import Request

from nucliadb.models.entities import EntitiesGroup
from nucliadb.models.labels import LabelSet
from nucliadb.models.resource import NucliaDBRoles
from nucliadb.models.widgets import Widget, WidgetMode
from nucliadb.writer.api.v1.router import KB_PREFIX, api
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    name="Set Knowledge Box Entities",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_entities(request: Request, kbid: str, group: str, item: EntitiesGroup):
    ingest = get_ingest()
    pbrequest: SetEntitiesRequest = SetEntitiesRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.group = group
    if item.title:
        pbrequest.entities.title = item.title
    if item.color:
        pbrequest.entities.color = item.color

    pbrequest.entities.custom = item.custom

    for key, entity in item.entities.items():
        entitypb = pbrequest.entities.entities[key]
        entitypb.value = entity.value
        entitypb.merged = entity.merged
        entitypb.represents.extend(entity.represents)

    set_info_on_span({"nuclia.kbid": kbid})

    status: OpStatusWriter = await ingest.SetEntities(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on settings entities on a Knowledge box"
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

    set_info_on_span({"nuclia.kbid": kbid})

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

    set_info_on_span({"nuclia.kbid": kbid})

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
    set_info_on_span({"nuclia.kbid": kbid})
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
    f"/{KB_PREFIX}/{{kbid}}/widget/{{widget}}",
    status_code=200,
    name="Set Knowledge Box Widgets",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def set_widget(request: Request, kbid: str, widget: str, item: Widget):
    ingest = get_ingest()
    pbrequest: SetWidgetsRequest = SetWidgetsRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.widget.id = widget
    set_info_on_span({"nuclia.kbid": kbid})
    if item.description:
        pbrequest.widget.description = item.description

    if item.mode:
        if item.mode == WidgetMode.BUTTON:
            pbrequest.widget.mode = WidgetPB.WidgetMode.BUTTON
        elif item.mode == WidgetMode.INPUT:
            pbrequest.widget.mode = WidgetPB.WidgetMode.INPUT
        elif item.mode == WidgetMode.FORM:
            pbrequest.widget.mode = WidgetPB.WidgetMode.FORM

    if item.features:
        pbrequest.widget.features.useFilters = item.features.useFilters
        pbrequest.widget.features.suggestEntities = item.features.suggestEntities
        pbrequest.widget.features.suggestSentences = item.features.suggestSentences
        pbrequest.widget.features.suggestParagraphs = item.features.suggestParagraphs
        pbrequest.widget.features.suggestLabels = item.features.suggestLabels
        pbrequest.widget.features.editLabels = item.features.editLabels
        pbrequest.widget.features.entityAnnotation = item.features.entityAnnotation

    for filter_input in item.filters:
        pbrequest.widget.filters.append(filter_input)

    for entity_input in item.topEntities:
        pbrequest.widget.topEntities.append(entity_input)

    pbrequest.widget.style.update(item.style)

    status: OpStatusWriter = await ingest.SetWidgets(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on settings widgets on a Knowledge box"
        )


@api.delete(
    f"/{KB_PREFIX}/{{kbid}}/widget/{{widget}}",
    status_code=200,
    name="Delete Knowledge Box Widget",
    tags=["Knowledge Box Services"],
    openapi_extra={"x-operation_order": 3},
)
@requires(NucliaDBRoles.WRITER)
@version(1)
async def delete_widget(request: Request, kbid: str, widget: str):
    ingest = get_ingest()
    pbrequest: DetWidgetsRequest = DetWidgetsRequest()
    pbrequest.kb.uuid = kbid
    pbrequest.widget = widget
    set_info_on_span({"nuclia.kbid": kbid})
    status: OpStatusWriter = await ingest.DelWidgets(pbrequest)  # type: ignore
    if status.status == OpStatusWriter.Status.OK:
        return None
    elif status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on deleting widget from a Knowledge box"
        )
    return Response(status_code=204)
