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
from nucliadb_protos.knowledgebox_pb2 import KnowledgeBoxID
from nucliadb_protos.writer_pb2 import (
    GetEntitiesGroupRequest,
    GetEntitiesGroupResponse,
    GetEntitiesRequest,
    GetEntitiesResponse,
    GetLabelSetRequest,
    GetLabelSetResponse,
    GetLabelsRequest,
    GetLabelsResponse,
    GetSynonymsResponse,
    GetVectorSetsRequest,
    GetVectorSetsResponse,
    GetWidgetRequest,
    GetWidgetResponse,
    GetWidgetsRequest,
    GetWidgetsResponse,
    ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse,
    OpStatusWriter,
)
from starlette.requests import Request

from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb_models.entities import EntitiesGroup, KnowledgeBoxEntities
from nucliadb_models.labels import KnowledgeBoxLabels, LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_models.vectors import VectorSet, VectorSets
from nucliadb_models.widgets import KnowledgeBoxWidgets, Widget, WidgetMode
from nucliadb_telemetry.utils import set_info_on_span
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroups",
    status_code=200,
    name="Get Knowledge Box Entities",
    response_model=KnowledgeBoxEntities,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_entities(
    request: Request, kbid: str, show_entities: bool = False
) -> KnowledgeBoxEntities:
    if show_entities:
        return await get_all_entities(kbid)
    else:
        return await list_entities_groups(kbid)


async def get_all_entities(kbid: str):
    """WARNING: this function is really costly due to how entities are retrieved
    from the node."""
    ingest = get_ingest()
    e_request: GetEntitiesRequest = GetEntitiesRequest()
    e_request.kb.uuid = kbid
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetEntitiesResponse = await ingest.GetEntities(e_request)  # type: ignore
    if kbobj.status == GetEntitiesResponse.Status.OK:
        response = KnowledgeBoxEntities(uuid=kbid)
        for key, group in kbobj.groups.items():
            entities_group = EntitiesGroup.from_message(group)
            if "" in entities_group.entities:
                del entities_group.entities[""]
            response.groups[key] = entities_group
        return response
    elif kbobj.status == GetEntitiesResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif kbobj.status == GetEntitiesResponse.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error while getting entities groups"
        )
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


async def list_entities_groups(kbid: str):
    ingest = get_ingest()
    e_request: ListEntitiesGroupsRequest = ListEntitiesGroupsRequest()
    e_request.kb.uuid = kbid
    set_info_on_span({"nuclia.kbid": kbid})

    entities_groups = await ingest.ListEntitiesGroups(e_request)  # type: ignore
    if entities_groups.status == ListEntitiesGroupsResponse.Status.OK:
        response = KnowledgeBoxEntities(uuid=kbid)
        for key, eg_summary in entities_groups.groups.items():
            entities_group = EntitiesGroup.from_summary_message(eg_summary)
            response.groups[key] = entities_group
        return response
    elif entities_groups.status == ListEntitiesGroupsResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif entities_groups.status == ListEntitiesGroupsResponse.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error while listing entities groups"
        )
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    name="Get Knowledge Box Entities",
    response_model=EntitiesGroup,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_entity(request: Request, kbid: str, group: str) -> EntitiesGroup:
    ingest = get_ingest()
    l_request: GetEntitiesGroupRequest = GetEntitiesGroupRequest()
    l_request.kb.uuid = kbid
    l_request.group = group
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetEntitiesGroupResponse = await ingest.GetEntitiesGroup(l_request)  # type: ignore
    if kbobj.status == GetEntitiesGroupResponse.Status.OK:
        response = EntitiesGroup.from_message(kbobj.group)
        return response
    elif kbobj.status == GetEntitiesGroupResponse.Status.KB_NOT_FOUND:
        raise HTTPException(
            status_code=404, detail=f"Knowledge Box '{kbid}' does not exist"
        )
    elif kbobj.status == GetEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND:
        raise HTTPException(
            status_code=404, detail=f"Entities group '{group}' does not exist"
        )
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/labelsets",
    status_code=200,
    name="Get Knowledge Box Labels",
    response_model=KnowledgeBoxLabels,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_labels(request: Request, kbid: str) -> KnowledgeBoxLabels:
    ingest = get_ingest()
    l_request: GetLabelsRequest = GetLabelsRequest()
    l_request.kb.uuid = kbid
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetLabelsResponse = await ingest.GetLabels(l_request)  # type: ignore
    if kbobj.status == GetLabelsResponse.Status.OK:
        response = KnowledgeBoxLabels(uuid=kbid)
        for labelset, labelset_data in kbobj.labels.labelset.items():
            labelset_response = LabelSet(
                **MessageToDict(
                    labelset_data,
                    preserving_proto_field_name=True,
                    including_default_value_fields=True,
                )
            )
            response.labelsets[labelset] = labelset_response
        return response
    elif kbobj.status == GetLabelsResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    name="Get Knowledge Box Label",
    response_model=LabelSet,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_label(request: Request, kbid: str, labelset: str) -> LabelSet:
    ingest = get_ingest()
    l_request: GetLabelSetRequest = GetLabelSetRequest()
    l_request.kb.uuid = kbid
    l_request.labelset = labelset
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetLabelSetResponse = await ingest.GetLabelSet(l_request)  # type: ignore
    if kbobj.status == GetLabelSetResponse.Status.OK:
        response = LabelSet(
            **MessageToDict(
                kbobj.labelset,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )
        return response
    elif kbobj.status == GetLabelSetResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/widgets",
    status_code=200,
    name="Get Knowledge Box Widgets",
    response_model=KnowledgeBoxWidgets,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_widgets(request: Request, kbid: str) -> KnowledgeBoxWidgets:
    ingest = get_ingest()
    l_request: GetWidgetsRequest = GetWidgetsRequest()
    l_request.kb.uuid = kbid
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetWidgetsResponse = await ingest.GetWidgets(l_request)  # type: ignore
    if kbobj.status == GetWidgetsResponse.Status.OK:
        response = KnowledgeBoxWidgets(uuid=kbid)
        for key, widget_obj in kbobj.widgets.items():
            widget = Widget(id=key)
            if widget_obj.id != key:
                raise HTTPException(
                    status_code=500, detail="Inconsistency on widget id"
                )
            widget.description = widget_obj.description
            if widget_obj.mode == 0:
                widget_mode = WidgetMode.BUTTON
            elif widget_obj.mode == 1:
                widget_mode = WidgetMode.INPUT
            elif widget_obj.mode == 2:
                widget_mode = WidgetMode.FORM
            widget.mode = widget_mode
            widget.features = MessageToDict(
                widget_obj.features,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )  # type: ignore
            widget.filters = [x for x in widget_obj.filters]
            widget.topEntities = [x for x in widget_obj.topEntities]
            widget.style = {x: y for x, y in widget_obj.style.items()}
            response.widgets[key] = widget
        return response
    elif kbobj.status == GetWidgetsResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/widget/{{widget}}",
    status_code=200,
    name="Get Knowledge Box Widgets",
    response_model=Widget,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_widget(request: Request, kbid: str, widget: str) -> Widget:
    ingest = get_ingest()
    l_request: GetWidgetRequest = GetWidgetRequest()
    l_request.kb.uuid = kbid
    l_request.widget = widget
    set_info_on_span({"nuclia.kbid": kbid})

    kbobj: GetWidgetResponse = await ingest.GetWidget(l_request)  # type: ignore
    if kbobj.status == GetWidgetResponse.Status.OK:
        response = Widget(id=kbobj.widget.id)
        response.description = kbobj.widget.description
        if kbobj.widget.mode == 0:
            widget_mode = WidgetMode.BUTTON
        elif kbobj.widget.mode == 1:
            widget_mode = WidgetMode.INPUT
        elif kbobj.widget.mode == 2:
            widget_mode = WidgetMode.FORM
        response.mode = widget_mode
        response.features = MessageToDict(
            kbobj.widget.features,
            preserving_proto_field_name=True,
            including_default_value_fields=True,
        )  # type: ignore
        response.filters = [x for x in kbobj.widget.filters]
        response.topEntities = [x for x in kbobj.widget.topEntities]
        response.style = {x: y for x, y in kbobj.widget.style.items()}
        return response
    elif kbobj.status == GetWidgetResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    else:
        raise HTTPException(status_code=500, detail="Unknown GRPC response")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/vectorsets",
    status_code=200,
    name="Get Knowledge Box VectorSet",
    tags=["Knowledge Box Services"],
    response_model=VectorSets,
    openapi_extra={"x-operation_order": 1},
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_vectorsets(request: Request, kbid: str):
    ingest = get_ingest()
    pbrequest: GetVectorSetsRequest = GetVectorSetsRequest()
    pbrequest.kb.uuid = kbid

    set_info_on_span({"nuclia.kbid": kbid})

    vectorsets: GetVectorSetsResponse = await ingest.GetVectorSets(pbrequest)  # type: ignore
    if vectorsets.status == GetVectorSetsResponse.Status.OK:
        result = VectorSets(vectorsets={})
        for key, vector in vectorsets.vectorsets.vectorsets.items():
            result.vectorsets[key] = VectorSet.from_message(vector)
        return result
    elif vectorsets.status == GetVectorSetsResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="VectorSet does not exist")
    elif vectorsets.status == GetVectorSetsResponse.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error on getting vectorset on a Knowledge box"
        )


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=200,
    name="Get Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    response_model=KnowledgeBoxSynonyms,
    openapi_extra={"x-operation_order": 2},
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_custom_synonyms(request: Request, kbid: str):
    set_info_on_span({"nuclia.kbid": kbid})
    ingest = get_ingest()
    pbrequest = KnowledgeBoxID(uuid=kbid)
    pbresponse: GetSynonymsResponse = await ingest.GetSynonyms(pbrequest)  # type: ignore
    if pbresponse.status.status == OpStatusWriter.Status.OK:
        return KnowledgeBoxSynonyms.from_message(pbresponse.synonyms)
    elif pbresponse.status.status == OpStatusWriter.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif pbresponse.status.status == OpStatusWriter.Status.ERROR:
        raise HTTPException(
            status_code=500, detail="Error getting synonyms of a Knowledge box"
        )
