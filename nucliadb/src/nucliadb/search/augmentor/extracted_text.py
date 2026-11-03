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
from contextvars import ContextVar

from nidx_protos.nidx_pb2 import ExtractedTextsRequest, ExtractedTextsResponse

from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb.search.augmentor.metrics import augmentor_observer


class ExtractedTexts:
    def __init__(self, responses: list[ExtractedTextsResponse]):
        self.responses = responses

    def get_field_text(self, id: FieldId) -> str | None:
        text = None
        for response in self.responses:
            text = response.fields.get(id.full_without_subfield())
            if text:
                break
        return text or None

    def get_paragraph_text(self, id: ParagraphId) -> str | None:
        text = None
        for response in self.responses:
            text = response.paragraphs.get(id.full())
            if text:
                break
        return text


index_et_cache: ContextVar[ExtractedTexts | None] = ContextVar("index_et_cache", default=None)


@augmentor_observer.wrap({"type": "index_extracted_texts"})
async def extracted_texts(
    kbid: str, fields: set[FieldId], paragraphs: set[ParagraphId]
) -> ExtractedTexts | None:
    if not fields and not paragraphs:
        return None

    requests = await _build_requests(kbid, fields, paragraphs)
    if requests is None:
        return None

    ops = [asyncio.create_task(_extracted_texts(kbid, request)) for request in requests]
    responses = await asyncio.gather(*ops)
    if not any(response.fields or response.paragraphs for response in responses):
        return None
    return ExtractedTexts(responses)


async def _extracted_texts(kbid: str, request: ExtractedTextsRequest) -> ExtractedTextsResponse:
    field_uris = {
        f"{field.rid}/{field.field_type}/{field.field_name}": MarkLogicURIs.field_processed(
            field.rid, field.field_name, field.field_type
        )
        for field in request.field_ids
    }
    paragraph_uris = {}
    for paragraph in request.paragraph_ids:
        field_id = FieldId(
            rid=paragraph.rid,
            type=paragraph.field_type,
            key=paragraph.field_name,
            subfield_id=paragraph.split or None,
        )
        paragraph_id = ParagraphId(
            field_id=field_id,
            paragraph_start=paragraph.paragraph_start,
            paragraph_end=paragraph.paragraph_end,
        )
        paragraph_uris[paragraph_id.full()] = (
            paragraph_id,
            MarkLogicURIs.chunk(field_id.rid, field_id.type, field_id.key, paragraph_id.full()),
        )
        field_uris[field_id.full_without_subfield()] = MarkLogicURIs.field_processed(
            field_id.rid, field_id.key, field_id.type
        )

    uris = set(field_uris.values()) | {uri for _, uri in paragraph_uris.values()}
    async with get_driver().ro_transaction(kbid=kbid) as txn:
        contents = await documents.read_many(txn, list(uris))

    response = ExtractedTextsResponse()
    for field in request.field_ids:
        key = f"{field.rid}/{field.field_type}/{field.field_name}"
        content = contents.get(field_uris[key])
        if content is not None:
            response.fields[key] = content["extracted_text"]
    for key, (paragraph_id, uri) in paragraph_uris.items():
        content = contents.get(uri)
        if content is not None:
            response.paragraphs[key] = content["text"]
        else:
            field_content = contents.get(field_uris[paragraph_id.field_id.full_without_subfield()])
            if field_content is not None:
                response.paragraphs[key] = field_content["extracted_text"][
                    paragraph_id.paragraph_start : paragraph_id.paragraph_end
                ]
    return response


async def _build_requests(
    kbid: str, fields: set[FieldId], paragraphs: set[ParagraphId]
) -> list[ExtractedTextsRequest] | None:
    request = ExtractedTextsRequest()
    for field_id in fields:
        request.field_ids.append(
            ExtractedTextsRequest.FieldId(
                rid=field_id.rid,
                field_type=field_id.type,
                field_name=field_id.key,
                split=field_id.subfield_id,
            )
        )
    for paragraph_id in paragraphs:
        request.paragraph_ids.append(
            ExtractedTextsRequest.ParagraphId(
                rid=paragraph_id.field_id.rid,
                field_type=paragraph_id.field_id.type,
                field_name=paragraph_id.field_id.key,
                split=paragraph_id.field_id.subfield_id,
                paragraph_start=paragraph_id.paragraph_start,
                paragraph_end=paragraph_id.paragraph_end,
            )
        )
    return [request]
