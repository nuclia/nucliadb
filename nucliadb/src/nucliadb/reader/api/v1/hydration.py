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
from enum import Enum
import time
from typing import Optional

from fastapi.requests import Request
from fastapi_versioning import version
from pydantic import BaseModel, Field, field_validator

from nucliadb.common.datamanagers.fields import KB_RESOURCE_FIELD
from nucliadb.common.datamanagers.resources import KB_RESOURCE_BASIC
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.ingest.fields.base import FieldTypes
from nucliadb_models.resource import NucliaDBRoles
from nucliadb_protos.resources_pb2 import ExtractedText
from nucliadb_utils.authentication import requires_one
from nucliadb_utils.storages.storage import Storage
from nucliadb_utils.utilities import get_storage

from .router import KB_PREFIX, api


class HydrateTextBlocksRequest(BaseModel):
    text_blocks: list[str] = Field(min_length=1, max_length=100)

    @field_validator("text_blocks", mode="before")
    def validate_text_block_id(cls, v):
        for text_block_id in v:
            try:
                ParagraphId.from_string(text_block_id)
            except Exception:
                raise ValueError("Invalid text block id format")
        return v


class HydratedTextBlock(BaseModel):
    id: str
    text: str


class HydrateErrorCode(str, Enum):
    UNKNOWN = "unknown"
    NOT_FOUND = "not_found"
    INVALID = "invalid"


class HydrateError(BaseModel):
    code: HydrateErrorCode
    message: Optional[str] = None


class HydrateTextBlocksResponse(BaseModel):
    text_blocks: dict[str, HydratedTextBlock]
    errors: dict[str, HydrateError]


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/hydrate/text_blocks",
    tags=["Resource fields"],
    status_code=200,
    summary="Hydrate text blocks",
)
@requires_one([NucliaDBRoles.READER])
@version(1)
async def hydrate_text_blocks_endpoint(
    request: Request,
    kbid: str,
    item: HydrateTextBlocksRequest,
) -> HydrateTextBlocksResponse:
    start = time.time()
    response = HydrateTextBlocksResponse(
        text_blocks={},
        errors={},
    )

    by_field = group_by_field(item)

    await check_fields_exist(
        kbid=kbid,
        by_field=by_field,
        response=response,
    )

    await hydrate_text_blocks(
        kbid=kbid,
        by_field=by_field,
        response=response,
    )
    print(f"Endpoint hydrate_text_blocks took {time.time() - start:.2f} seconds")
    return response


def group_by_field(
    item: HydrateTextBlocksRequest,
) -> dict[FieldId, set[ParagraphId]]:
    by_field: dict[FieldId, set[ParagraphId]] = {}
    for text_block_id in item.text_blocks:
        parsed = ParagraphId.from_string(text_block_id)
        by_field.setdefault(parsed.field_id, set()).add(parsed)
    return by_field


async def check_fields_exist(
    kbid: str,
    by_field: dict[FieldId, set[ParagraphId]],
    response: HydrateTextBlocksResponse,
):
    start = time.time()
    # Prepare keys to check existence
    keys_to_check: dict[str, FieldId] = {}
    for field_id in by_field.keys():
        if field_id.type == "a":
            key = KB_RESOURCE_BASIC.format(
                kbid=kbid,
                uuid=field_id.rid,
            )
        else:
            key = KB_RESOURCE_FIELD.format(
                kbid=kbid,
                uuid=field_id.rid,
                type=field_id.type,
                field=field_id.key,
            )
        keys_to_check[key] = field_id

    # Check existence of fields in the database
    not_exsisting_fields: list[FieldId] = []
    async with get_driver().transaction(read_only=True) as txn:
        results = await txn.batch_get(list(keys_to_check.keys()), for_update=False)
        for key, serialized in zip(keys_to_check.keys(), results):
            if serialized is None:
                not_exsisting_fields.append(keys_to_check[key])

    # Handle non-existing fields
    for field_id in not_exsisting_fields:
        for tbid in by_field.get(field_id, []):
            response.errors[str(tbid)] = HydrateError(
                code=HydrateErrorCode.NOT_FOUND,
                message="Field or resource not found",
            )
        by_field.pop(field_id, None)
    print(f"Checked existence of {len(keys_to_check)} fields in {time.time() - start:.2f} seconds")


async def hydrate_text_blocks(
    kbid: str,
    by_field: dict[FieldId, set[ParagraphId]],
    response: HydrateTextBlocksResponse,
):
    max_parallel_hydrations = asyncio.Semaphore(5)
    storage = await get_storage()
    tasks = []
    for field_id, paragraph_ids in by_field.items():
        tasks.append(
            hydrate_field(
                storage=storage,
                kbid=kbid,
                field_id=field_id,
                paragraph_ids=paragraph_ids,
                response=response,
                semaphore=max_parallel_hydrations,
            )
        )
    await asyncio.gather(*tasks)


async def hydrate_field(
    storage: Storage,
    kbid: str,
    field_id: FieldId,
    paragraph_ids: set[ParagraphId],
    response: HydrateTextBlocksResponse,
    semaphore: asyncio.Semaphore,
):
    start = time.time()
    await _hydrate_field(
        storage=storage,
        kbid=kbid,
        field_id=field_id,
        paragraph_ids=paragraph_ids,
        response=response,
        semaphore=semaphore,
    )
    print(f"Hydrated field {field_id} in {time.time() - start:.2f} seconds")

async def _hydrate_field(
    storage: Storage,
    kbid: str,
    field_id: FieldId,
    paragraph_ids: set[ParagraphId],
    response: HydrateTextBlocksResponse,
    semaphore: asyncio.Semaphore,
):
    async with semaphore:
        extracted_text = await download_field_extracted_text(
            storage=storage,
            kbid=kbid,
            field_id=field_id,
        )
        if extracted_text is None:
            for pid in paragraph_ids:
                response.errors[pid.full()] = HydrateError(
                    code=HydrateErrorCode.NOT_FOUND,
                    message="Field text not found",
                )
            return
        for pid in paragraph_ids:
            subfield = pid.field_id.subfield_id
            if subfield is not None:
                if subfield not in extracted_text.split_text:
                    response.errors[str(pid)] = HydrateError(
                        code=HydrateErrorCode.NOT_FOUND,
                        message="Subfield text not found",
                    )
                    continue
                text = extracted_text.split_text[subfield]
            else:
                text = extracted_text.text
            ptext = text[pid.paragraph_start : pid.paragraph_end]
            if not ptext:
                response.errors[str(pid)] = HydrateError(
                    code=HydrateErrorCode.NOT_FOUND,
                    message="Text block out of range",
                )
                continue
            response.text_blocks[str(pid)] = HydratedTextBlock(
                id=str(pid),
                text=ptext,
            )


async def download_field_extracted_text(
    storage: Storage,
    kbid: str,
    field_id: FieldId,
) -> Optional[ExtractedText]:
    """
    Download the extracted text for a given field.
    """
    start = time.time()
    sf = storage.file_extracted(
        kbid=kbid,
        uuid=field_id.rid,
        field_type=field_id.type,
        field=field_id.key,
        key=FieldTypes.FIELD_TEXT.value,
    )
    et = await storage.download_pb(sf, ExtractedText)
    print(f"Downloaded extracted text for field {field_id}: {time.time() - start:.2f} seconds")
    return et
