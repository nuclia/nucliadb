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

from nidx_protos.nidx_pb2 import ExtractedTextsRequest, ExtractedTextsResponse
from nidx_protos.nidx_pb2_grpc import NidxSearcherStub

from nucliadb.common import datamanagers
from nucliadb.common.ids import FieldId, ParagraphId
from nucliadb.common.nidx import get_nidx_searcher_client
from nucliadb.search import logger
from nucliadb.search.augmentor.metrics import augmentor_observer


class ExtractedTexts:
    def __init__(self, nidx_responses: list[ExtractedTextsResponse]):
        self.responses = nidx_responses

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


@augmentor_observer.wrap({"type": "nidx_extracted_texts"})
async def extracted_texts(
    kbid: str, fields: set[FieldId], paragraphs: set[ParagraphId]
) -> ExtractedTexts | None:
    nidx_searcher = get_nidx_searcher_client()
    requests = await _build_requests(kbid, fields, paragraphs)
    if requests is None:
        return None

    ops = [asyncio.create_task(_extracted_texts(nidx_searcher, request)) for request in requests]
    responses = await asyncio.gather(*ops)
    return ExtractedTexts(responses)


async def _extracted_texts(
    nidx_searcher: NidxSearcherStub, request: ExtractedTextsRequest
) -> ExtractedTextsResponse:
    # wrapper to help recognize the gRPC call as a coroutine
    return await nidx_searcher.ExtractedTexts(request)


async def _build_requests(
    kbid: str, fields: set[FieldId], paragraphs: set[ParagraphId]
) -> list[ExtractedTextsRequest] | None:
    # shard_id -> nidx gRPC request
    nidx_requests: dict[str, ExtractedTextsRequest] = {}

    async with datamanagers.with_ro_transaction() as txn:
        kb_shards = await datamanagers.cluster.get_kb_shards(txn, kbid=kbid)
        if kb_shards is None:
            return None

        logical_to_nidx_shard = {}
        for shard in kb_shards.shards:
            logical_to_nidx_shard[shard.shard] = shard.nidx_shard_id

        resource_nidx_shard = {}

        for field_id in fields:
            rid = field_id.rid
            if rid not in resource_nidx_shard:
                resource_shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=kbid, rid=rid
                )
                if resource_shard_id is None:
                    # Resource not in DB, either a dirty read (reading a delete)
                    # or a user requesting a deleted or wrong resource. Skip
                    continue
                nidx_shard_id = logical_to_nidx_shard.get(resource_shard_id)
                if nidx_shard_id is None:
                    logger.warning(
                        "nidx shard not found for resource shard",
                        extra={"kbid": kbid, "resource_shard_id": resource_shard_id},
                    )
                    return None

                resource_nidx_shard[rid] = nidx_shard_id

            nidx_shard_id = resource_nidx_shard[rid]
            request = nidx_requests.setdefault(nidx_shard_id, ExtractedTextsRequest())
            request.shard_id = nidx_shard_id
            request.field_ids.append(
                ExtractedTextsRequest.FieldId(
                    rid=field_id.rid,
                    field_type=field_id.type,
                    field_name=field_id.key,
                    split=field_id.subfield_id,
                )
            )

        for paragraph_id in paragraphs:
            rid = paragraph_id.rid
            if rid not in resource_nidx_shard:
                resource_shard_id = await datamanagers.resources.get_resource_shard_id(
                    txn, kbid=kbid, rid=rid
                )
                if resource_shard_id is None:
                    # Resource not in DB, either a dirty read (reading a delete)
                    # or a user requesting a deleted or wrong resource. Skip
                    continue
                nidx_shard_id = logical_to_nidx_shard.get(resource_shard_id)
                if nidx_shard_id is None:
                    logger.warning(
                        "nidx shard not found for resource shard",
                        extra={"kbid": kbid, "resource_shard_id": resource_shard_id},
                    )
                    return None

                resource_nidx_shard[rid] = nidx_shard_id

            nidx_shard_id = resource_nidx_shard[rid]
            request = nidx_requests.setdefault(nidx_shard_id, ExtractedTextsRequest())
            request.shard_id = nidx_shard_id
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

    return list(nidx_requests.values())
