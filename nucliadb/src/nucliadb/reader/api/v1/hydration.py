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
from enum import Enum
from typing import Optional

from fastapi.requests import Request
from fastapi_versioning import version
from pydantic import BaseModel, Field

from nucliadb.common.datamanagers.fields import KB_RESOURCE_FIELD
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
    text_blocks: list[str] = Field(max_length=100)


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
async def hydrate_text_blocks(
    request: Request,
    kbid: str,
    item: HydrateTextBlocksRequest,
) -> HydrateTextBlocksResponse:
    response = HydrateTextBlocksResponse(
        text_blocks={},
        errors={},
    )
    by_field = group_by_field(item, response)
    await check_fields_exist(
        kbid=kbid,
        by_field=by_field,
        response=response,
    )
    storage = await get_storage()
    for field_id, paragraph_ids in by_field.items():
        await hydrate_field(
            storage=storage,
            kbid=kbid,
            field_id=field_id,
            paragraph_ids=paragraph_ids,
            response=response,
        )
    return response


def group_by_field(
    item: HydrateTextBlocksRequest,
    response: HydrateTextBlocksResponse,
) -> dict[FieldId, list[ParagraphId]]:
    by_field: dict[FieldId, list[ParagraphId]] = {}
    for text_block_id in item.text_blocks:
        try:
            parsed = ParagraphId.from_string(text_block_id)
            by_field.setdefault(parsed.field_id, []).append(parsed)
        except ValueError:
            response.errors[text_block_id] = HydrateError(
                code=HydrateErrorCode.INVALID,
            )
    return by_field


async def check_fields_exist(
    kbid: str,
    by_field: dict[FieldId, list[ParagraphId]],
    response: HydrateTextBlocksResponse,
):
    keys_to_check: dict[str, FieldId] = {}
    for field_id in by_field.keys():
        key = KB_RESOURCE_FIELD.format(
            kbid=kbid,
            uuid=field_id.rid,
            type=field_id.type,
            field=field_id.key,
        )
        keys_to_check[key] = field_id

    not_exsisting_fields: list[FieldId] = []
    async with get_driver().transaction(read_only=True) as txn:
        results = await txn.batch_get(list(keys_to_check.keys()), for_update=False)
        for key, serialized in zip(keys_to_check.keys(), results):
            if serialized is None:
                not_exsisting_fields.append(keys_to_check[key])

    for field_id in not_exsisting_fields:
        for tbid in by_field.get(field_id, []):
            response.errors[str(tbid)] = HydrateError(
                code=HydrateErrorCode.NOT_FOUND,
                message="Field or resource not found",
            )
        by_field.pop(field_id, None)


async def hydrate_field(
    storage: Storage,
    kbid: str,
    field_id: FieldId,
    paragraph_ids: list[ParagraphId],
    response: HydrateTextBlocksResponse,
):
    sf = storage.file_extracted(kbid, field_id.rid, field_id.type, field_id.key, FieldTypes.FIELD_TEXT)
    payload: Optional[ExtractedText] = await storage.download_pb(sf, ExtractedText)
    if payload is None:
        for pid in paragraph_ids:
            response.errors[pid.full()] = HydrateError(
                code=HydrateErrorCode.NOT_FOUND,
                message="Field text not found",
            )
        return
    for pid in paragraph_ids:
        subfield = pid.field_id.subfield_id
        if subfield is not None:
            if subfield not in payload.split_text:
                response.errors[str(pid)] = HydrateError(
                    code=HydrateErrorCode.NOT_FOUND,
                    message="Subfield text not found",
                )
                continue
            text = payload.split_text[subfield]
        else:
            text = payload.text
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
