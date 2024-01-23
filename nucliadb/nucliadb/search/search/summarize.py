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

from nucliadb.common.datamanagers.exceptions import KnowledgeBoxNotFound
from nucliadb.common.datamanagers.kb import KnowledgeBoxDataManager
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import Field
from nucliadb.ingest.orm.knowledgebox import KnowledgeBox
from nucliadb.ingest.orm.resource import Resource
from nucliadb.search import logger
from nucliadb.search.utilities import get_predict
from nucliadb_models.search import (
    SummarizedResponse,
    SummarizeModel,
    SummarizeRequest,
    SummarizeResourceModel,
)
from nucliadb_utils.utilities import get_storage

ExtractedTexts = list[tuple[str, str, Optional[ExtractedText]]]

MAX_GET_EXTRACTED_TEXT_OPS = 20


async def summarize(kbid: str, request: SummarizeRequest) -> SummarizedResponse:
    predict_request = SummarizeModel()
    predict_request.user_prompt = request.user_prompt
    predict_request.summary_kind = request.summary_kind

    for rid, field_id, extracted_text in await get_extracted_texts(
        kbid, request.resources
    ):
        if extracted_text is None:
            continue

        fields = predict_request.resources.setdefault(
            rid, SummarizeResourceModel()
        ).fields
        fields[field_id] = extracted_text.text

    predict = get_predict()
    return await predict.summarize(kbid, predict_request)


async def get_extracted_texts(kbid: str, resource_uuids: list[str]) -> ExtractedTexts:
    results: ExtractedTexts = []

    driver = get_driver()
    storage = await get_storage()

    kbm = KnowledgeBoxDataManager(driver)
    if not await kbm.exists_kb(kbid):
        raise KnowledgeBoxNotFound(kbid)

    max_tasks = asyncio.Semaphore(MAX_GET_EXTRACTED_TEXT_OPS)
    tasks = []

    # Schedule getting extracted text for each field of each resource
    async with driver.transaction() as txn:
        kb_orm = KnowledgeBox(txn, storage, kbid)
        for rid in set(resource_uuids):
            resource_orm = Resource(txn=txn, storage=storage, kb=kb_orm, uuid=rid)
            fields = await resource_orm.get_fields(force=True)
            for _, field in fields.items():
                task = asyncio.create_task(get_extracted_text(rid, field, max_tasks))
                tasks.append(task)

        if len(tasks) == 0:
            # No extracted text to get
            return results

        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)

    # Parse the task results
    for done_task in done:
        if done_task.exception() is not None:  # pragma: no cover
            exception = done_task.exception()
            logger.error("Error fetching extracted text", exc_info=exception)
            for pending_task in pending:
                pending_task.cancel()
            raise exception  # type: ignore
        results.append(done_task.result())

    tasks.clear()
    return results


async def get_extracted_text(
    rid: str, field: Field, max_operations: asyncio.Semaphore
) -> tuple[str, str, Optional[ExtractedText]]:
    async with max_operations:
        extracted_text = await field.get_extracted_text(force=True)
        field_key = f"{field.type}/{field.id}"
        return rid, field_key, extracted_text
