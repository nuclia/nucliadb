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
from typing import Union

from fastapi import Header, Request, Response
from fastapi_versioning import version

from nucliadb.common.models_utils import from_proto
from nucliadb.models.responses import HTTPClientError
from nucliadb.search.api.v1.resource.utils import get_resource_uuid_by_slug
from nucliadb.search.api.v1.router import KB_PREFIX, RESOURCE_PREFIX, RESOURCE_SLUG_PREFIX, api
from nucliadb.search.predict_models import AugmentedField, RunAgentsResponse
from nucliadb.search.search.exceptions import ResourceNotFoundError
from nucliadb.search.search.ingestion_agents import run_agents
from nucliadb_models.agents.ingestion import (
    AppliedDataAugmentation,
    NewTextField,
    ResourceAgentsRequest,
    ResourceAgentsResponse,
)
from nucliadb_models.agents.ingestion import AugmentedField as PublicAugmentedField
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_utils.authentication import requires_one


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_PREFIX}/{{rid}}/run-agents",
    status_code=200,
    summary="Run Agents on Resource",
    description="Run Agents on Resource",
    tags=["Ingestion Agents"],
    response_model_exclude_unset=True,
    response_model=ResourceAgentsResponse,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def run_agents_by_uuid(
    request: Request,
    response: Response,
    kbid: str,
    rid: str,
    item: ResourceAgentsRequest,
    x_nucliadb_user: str = Header(""),
) -> Union[ResourceAgentsResponse, HTTPClientError]:
    return await _run_agents_endpoint(kbid, rid, x_nucliadb_user, item)


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/{RESOURCE_SLUG_PREFIX}/{{slug}}/run-agents",
    status_code=200,
    summary="Run Agents on Resource (by slug)",
    description="Run Agents on Resource (by slug)",
    tags=["Ingestion Agents"],
    response_model_exclude_unset=True,
    response_model=ResourceAgentsResponse,
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def run_agents_by_slug(
    request: Request,
    response: Response,
    kbid: str,
    slug: str,
    item: ResourceAgentsRequest,
    x_nucliadb_user: str = Header(""),
) -> Union[ResourceAgentsResponse, HTTPClientError]:
    resource_id = await get_resource_uuid_by_slug(kbid, slug)
    if resource_id is None:
        return HTTPClientError(status_code=404, detail="Resource not found")
    return await _run_agents_endpoint(kbid, resource_id, x_nucliadb_user, item)


async def _run_agents_endpoint(
    kbid: str, resource_id: str, user_id: str, item: ResourceAgentsRequest
) -> Union[ResourceAgentsResponse, HTTPClientError]:
    try:
        run_agents_response: RunAgentsResponse = await run_agents(
            kbid,
            resource_id,
            user_id,
            filters=item.filters,
            agent_ids=item.agent_ids,
        )
    except ResourceNotFoundError:
        return HTTPClientError(status_code=404, detail="Resource not found")
    response = ResourceAgentsResponse(results={})
    for field_id, augmented_field in run_agents_response.results.items():
        response.results[field_id] = _parse_augmented_field(augmented_field)
    return response


def _parse_augmented_field(augmented_field: AugmentedField) -> PublicAugmentedField:
    ada = augmented_field.applied_data_augmentation
    return PublicAugmentedField(
        metadata=from_proto.field_metadata(augmented_field.metadata),
        applied_data_augmentation=AppliedDataAugmentation(
            changed=ada.changed,
            qas=from_proto.question_answers(ada.qas) if ada.qas else None,
            new_text_fields=[
                NewTextField(
                    text_field=from_proto.field_text(ntf["text_field"]),
                    destination=ntf["destination"],
                )
                for ntf in ada.new_text_fields
            ],
        ),
        input_nuclia_tokens=augmented_field.input_nuclia_tokens,
        output_nuclia_tokens=augmented_field.output_nuclia_tokens,
        time=augmented_field.time,
    )
