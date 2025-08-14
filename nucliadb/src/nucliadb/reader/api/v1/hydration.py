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


class HydrateTextBlocksRespone(BaseModel):
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
) -> HydrateTextBlocksRespone:
    """
    Hydrate text blocks in a resource.
    """
    response = HydrateTextBlocksRespone(
        text_blocks={},
        errors={},
    )

    # Validate the text block ids and group them by resource and field
    by_field: dict[FieldId, list[ParagraphId]] = {}
    for text_block_id in item.text_blocks:
        try:
            parsed = ParagraphId.from_string(text_block_id)
            by_field.setdefault(parsed.field_id, []).append(parsed)
        except ValueError:
            response.errors[text_block_id] = HydrateError(
                code=HydrateErrorCode.INVALID,
            )

    if len(by_field) == 0:
        return response

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

    if len(by_field) == 0:
        return response

    # Now try to get the field objects, and discard those that are not found
    storage = await get_storage()
    for field_id, paragraph_ids in by_field.items():
        sf = storage.file_extracted(
            kbid, field_id.rid, field_id.type, field_id.key, FieldTypes.FIELD_TEXT
        )
        payload: Optional[ExtractedText] = await storage.download_pb(sf, ExtractedText)
        if payload is None:
            for pid in paragraph_ids:
                response.errors[pid.full()] = HydrateError(
                    code=HydrateErrorCode.NOT_FOUND,
                    message="Field text not found",
                )
        else:
            for pid in paragraph_ids:
                if pid.field_id.subfield_id is not None:
                    text = payload.split_text.get(pid.field_id.subfield_id) or ""
                else:
                    text = payload.text
                ptext = text[pid.paragraph_start : pid.paragraph_end]
                if not ptext:
                    response.errors[str(pid)] = HydrateError(
                        code=HydrateErrorCode.NOT_FOUND,
                        message="Subfield text not found",
                    )
                else:
                    response.text_blocks[str(pid)] = HydratedTextBlock(
                        id=str(pid),
                        text=ptext,
                    )

    return response
