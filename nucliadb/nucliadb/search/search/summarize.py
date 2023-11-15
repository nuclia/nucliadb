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
from typing import Optional

from nucliadb_protos.utils_pb2 import ExtractedText

from nucliadb.common.datamanagers.resources import ResourcesDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.search import logger
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    SummarizedResponse,
    SummarizeModel,
    SummarizeRequest,
    SummarizeResourceModel,
)
from nucliadb_utils.utilities import get_storage

MAX_GET_EXTRACTED_TEXT_OPS = 5


async def summarize(kbid: str, request: SummarizeRequest) -> SummarizedResponse:
    predict_request = SummarizeModel()

    driver = get_driver()
    storage = await get_storage()
    rdm = ResourcesDataManager(driver, storage)

    max_tasks = asyncio.Semaphore(MAX_GET_EXTRACTED_TEXT_OPS)
    tasks = []

    # Schedule fetching extracted texts in async tasks
    for resource_uuid in set(request.resources):
        resource = await rdm.get_resource(kbid, resource_uuid)
        if resource is None:
            continue

        predict_request.resources[resource_uuid] = SummarizeResourceModel()
        fields = await resource.get_fields(force=True)
        for _, field in fields.items():
            task = asyncio.create_task(get_extracted_text(field, max_tasks))
            tasks.append(task)

    # Parse the task results
    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    done_task: asyncio.Task
    for done_task in done:
        task_exception = done_task.exception()
        if task_exception is not None:  # pragma: no cover
            logger.error("Error fetching extracted text", exc_info=task_exception)
            pending_task: asyncio.Task
            for pending_task in pending:
                pending_task.cancel()
            raise task_exception

        extracted_text, field = done_task.result()
        if extracted_text is None:
            continue

        field_key = f"{field.type}/{field.id}"
        predict_request.resources[resource_uuid].fields[field_key] = extracted_text.text

    predict = get_predict()
    return await predict.summarize(kbid, predict_request)


async def get_extracted_text(
    field: Field, max_operations: asyncio.Semaphore
) -> tuple[Optional[ExtractedText], Field]:
    async with max_operations:
        extracted_text = await field.get_extracted_text(force=True)
        return extracted_text, field
