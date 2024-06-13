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
from typing import Optional, Union

from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from fastapi_versioning import version
from google.protobuf.json_format import MessageToDict
from starlette.requests import Request

from nucliadb.common import datamanagers
from nucliadb.common.cluster.settings import in_standalone_mode
from nucliadb.common.context.fastapi import get_app_context
from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.http_clients import processing
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.models.responses import HTTPClientError
from nucliadb.reader import SERVICE_NAME
from nucliadb.reader.api.v1.router import KB_PREFIX, api
from nucliadb.reader.reader.notifications import kb_notifications_stream
from nucliadb_models.entities import (
    EntitiesGroup,
    EntitiesGroupSummary,
    KnowledgeBoxEntities,
)
from nucliadb_models.labels import KnowledgeBoxLabels, LabelSet
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_models.synonyms import KnowledgeBoxSynonyms
from nucliadb_protos import writer_pb2
from nucliadb_protos.knowledgebox_pb2 import Synonyms
from nucliadb_protos.writer_pb2 import (
    GetEntitiesGroupRequest,
    GetEntitiesGroupResponse,
    ListEntitiesGroupsRequest,
    ListEntitiesGroupsResponse,
)
from nucliadb_utils.authentication import requires
from nucliadb_utils.utilities import get_ingest, get_storage


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroups",
    status_code=200,
    summary="Get Knowledge Box Entities",
    response_model=KnowledgeBoxEntities,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_entities(
    request: Request, kbid: str, show_entities: bool = False
) -> Union[KnowledgeBoxEntities, HTTPClientError]:
    if show_entities:
        return HTTPClientError(
            status_code=400,
            detail="show_entities param is not supported. Please use /entitiesgroup/{group} instead.",
        )
    return await list_entities_groups(kbid)


async def list_entities_groups(kbid: str):
    ingest = get_ingest()
    e_request: ListEntitiesGroupsRequest = ListEntitiesGroupsRequest()
    e_request.kb.uuid = kbid

    entities_groups = await ingest.ListEntitiesGroups(e_request)  # type: ignore
    if entities_groups.status == ListEntitiesGroupsResponse.Status.OK:
        response = KnowledgeBoxEntities(uuid=kbid)
        for key, eg_summary in entities_groups.groups.items():
            entities_group = EntitiesGroupSummary.from_message(eg_summary)
            response.groups[key] = entities_group
        return response
    elif entities_groups.status == ListEntitiesGroupsResponse.Status.NOTFOUND:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    elif entities_groups.status == ListEntitiesGroupsResponse.Status.ERROR:
        raise HTTPException(status_code=500, detail="Error while listing entities groups")
    else:
        raise HTTPException(status_code=500, detail="Error on listing Knowledge box entities")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/entitiesgroup/{{group}}",
    status_code=200,
    summary="Get a Knowledge Box Entities Group",
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

    kbobj: GetEntitiesGroupResponse = await ingest.GetEntitiesGroup(l_request)  # type: ignore
    if kbobj.status == GetEntitiesGroupResponse.Status.OK:
        response = EntitiesGroup.from_message(kbobj.group)
        return response
    elif kbobj.status == GetEntitiesGroupResponse.Status.KB_NOT_FOUND:
        raise HTTPException(status_code=404, detail=f"Knowledge Box '{kbid}' does not exist")
    elif kbobj.status == GetEntitiesGroupResponse.Status.ENTITIES_GROUP_NOT_FOUND:
        raise HTTPException(status_code=404, detail=f"Entities group '{group}' does not exist")
    else:
        raise HTTPException(status_code=500, detail="Error on getting entities group on a Knowledge box")


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/labelsets",
    status_code=200,
    summary="Get Knowledge Box Label Sets",
    response_model=KnowledgeBoxLabels,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_labelsets_endoint(request: Request, kbid: str) -> KnowledgeBoxLabels:
    try:
        return await get_labelsets(kbid)
    except KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")


async def get_labelsets(kbid: str) -> KnowledgeBoxLabels:
    kb_exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    if not kb_exists:
        raise KnowledgeBoxNotFound()
    labelsets: writer_pb2.Labels = await datamanagers.atomic.labelset.get_all(kbid=kbid)
    response = KnowledgeBoxLabels(uuid=kbid)
    for labelset, labelset_data in labelsets.labelset.items():
        labelset_response = LabelSet(
            **MessageToDict(
                labelset_data,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )
        response.labelsets[labelset] = labelset_response
    return response


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/labelset/{{labelset}}",
    status_code=200,
    summary="Get a Knowledge Box Label Set",
    response_model=LabelSet,
    tags=["Knowledge Box Services"],
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_labelset_endpoint(request: Request, kbid: str, labelset: str) -> LabelSet:
    try:
        return await get_labelset(kbid, labelset)
    except KnowledgeBoxNotFound:
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")


async def get_labelset(kbid: str, labelset_id: str) -> LabelSet:
    kb_exists = await datamanagers.atomic.kb.exists_kb(kbid=kbid)
    if not kb_exists:
        raise KnowledgeBoxNotFound()
    labelset: Optional[writer_pb2.LabelSet] = await datamanagers.atomic.labelset.get(
        kbid=kbid, labelset_id=labelset_id
    )
    if labelset is None:
        response = LabelSet()
    else:
        response = LabelSet(
            **MessageToDict(
                labelset,
                preserving_proto_field_name=True,
                including_default_value_fields=True,
            )
        )
    return response


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/custom-synonyms",
    status_code=200,
    summary="Get Knowledge Box Custom Synonyms",
    tags=["Knowledge Box Services"],
    response_model=KnowledgeBoxSynonyms,
    openapi_extra={"x-operation_order": 2},
)
@requires(NucliaDBRoles.READER)
@version(1)
async def get_custom_synonyms(request: Request, kbid: str):
    if not await datamanagers.atomic.kb.exists_kb(kbid=kbid):
        raise HTTPException(status_code=404, detail="Knowledge Box does not exist")
    synonyms = await datamanagers.atomic.synonyms.get(kbid=kbid) or Synonyms()
    return KnowledgeBoxSynonyms.from_message(synonyms)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/notifications",
    status_code=200,
    summary="Knowledge Box Notifications Stream",
    description="Provides a stream of activity notifications for the given Knowledge Box. The stream will be automatically closed after 2 minutes.",  # noqa: E501
    tags=["Knowledge Box Services"],
    response_description="Each line of the response is a Base64-encoded JSON object representing a notification. Refer to [the internal documentation](https://github.com/nuclia/nucliadb/blob/main/docs/tutorials/KB_NOTIFICATIONS.md) for a more detailed explanation of each notification type.",  # noqa: E501
    response_model=None,
    responses={"404": {"description": "Knowledge Box not found"}},
)
@requires(NucliaDBRoles.READER)
@version(1)
async def notifications_endpoint(
    request: Request, kbid: str
) -> Union[StreamingResponse, HTTPClientError]:
    if in_standalone_mode():
        return HTTPClientError(
            status_code=404,
            detail="Notifications are only available in the cloud offering of NucliaDB.",
        )

    context = get_app_context(request.app)

    if not await exists_kb(kbid=kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    response = StreamingResponse(
        content=kb_notifications_stream(context, kbid),
        status_code=200,
        media_type="binary/octet-stream",
    )

    return response


async def exists_kb(kbid: str) -> bool:
    async with datamanagers.with_ro_transaction() as txn:
        return await datamanagers.kb.exists_kb(txn, kbid=kbid)


@api.get(
    f"/{KB_PREFIX}/{{kbid}}/processing-status",
    status_code=200,
    summary="Knowledge Box Processing Status",
    description="Provides the status of the processing of the given Knowledge Box.",
    tags=["Knowledge Box Services"],
    response_model=processing.RequestsResults,
    responses={
        "404": {"description": "Knowledge Box not found"},
    },
)
@requires(NucliaDBRoles.READER)
@version(1)
async def processing_status(
    request: Request,
    kbid: str,
    cursor: Optional[str] = None,
    scheduled: Optional[bool] = None,
    limit: int = 20,
) -> Union[processing.RequestsResults, HTTPClientError]:
    if not await exists_kb(kbid=kbid):
        return HTTPClientError(status_code=404, detail="Knowledge Box not found")

    async with processing.ProcessingHTTPClient() as client:
        results = await client.requests(cursor=cursor, scheduled=scheduled, kbid=kbid, limit=limit)

    storage = await get_storage(service_name=SERVICE_NAME)
    driver = get_driver()

    async with driver.transaction(wait_for_abort=False, read_only=True) as txn:
        kb = KnowledgeBox(txn, storage, kbid)

        max_simultaneous = asyncio.Semaphore(10)

        async def _composition(
            result: processing.RequestsResult,
        ) -> Optional[processing.RequestsResult]:
            async with max_simultaneous:
                resource = await kb.get(result.resource_id)
                if resource is None:
                    return None

                basic = await resource.get_basic()
                if basic is None:
                    return None

                result.title = basic.title
                return result

        result_items = [
            item
            for item in await asyncio.gather(*[_composition(result) for result in results.results])
            if item is not None
        ]

    # overwrite result with only resources that exist in the database.
    results.results = result_items
    return results
