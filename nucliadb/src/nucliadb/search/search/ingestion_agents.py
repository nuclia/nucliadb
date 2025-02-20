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
from base64 import b64encode
from typing import Optional

from nucliadb.common import datamanagers
from nucliadb.ingest.fields.base import Field
from nucliadb.search.predict_models import (
    FieldInfo,
    NameOperationFilter,
    OperationType,
    RunAgentsRequest,
    RunAgentsResponse,
)
from nucliadb.search.search.exceptions import ResourceNotFoundError
from nucliadb.search.utilities import get_predict
from nucliadb_models.agents.ingestion import AgentsFilter
from nucliadb_protos.resources_pb2 import FieldMetadata


async def run_agents(
    kbid: str, rid: str, user_id: str, filters: Optional[list[AgentsFilter]] = None
) -> RunAgentsResponse:
    fields = await fetch_resource_fields(kbid, rid)

    item = RunAgentsRequest(user_id=user_id, filters=_parse_filters(filters), fields=fields)

    predict = get_predict()
    return await predict.run_agents(kbid, item)


def _parse_filters(filters: Optional[list[AgentsFilter]]) -> Optional[list[NameOperationFilter]]:
    if filters is None:
        return None
    return [
        NameOperationFilter(
            operation_type=OperationType(filter.type.value), task_names=filter.task_names
        )
        for filter in filters
    ]


async def fetch_resource_fields(kbid: str, rid: str) -> list[FieldInfo]:
    async with datamanagers.with_ro_transaction() as txn:
        resource = await datamanagers.resources.get_resource(txn, kbid=kbid, rid=rid)
        if resource is None:
            raise ResourceNotFoundError()
        fields = await resource.get_fields(force=True)

    tasks: list[asyncio.Task] = []
    for field in fields.values():
        tasks.append(asyncio.create_task(_hydrate_field_info(field)))
    await asyncio.gather(*tasks)

    return [task.result() for task in tasks]


async def _hydrate_field_info(field_obj: Field) -> FieldInfo:
    extracted_text = await field_obj.get_extracted_text(force=True)
    fcmw = await field_obj.get_field_metadata(force=True)
    if fcmw is None:
        metadata = FieldMetadata()
    else:
        metadata = fcmw.metadata
    serialized_metadata = b64encode(metadata.SerializeToString()).decode()
    return FieldInfo(
        text=extracted_text.text if extracted_text is not None else "",
        metadata=serialized_metadata,
        field_id=f"{field_obj.type}/{field_obj.id}",
    )
