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
from time import time

from fastapi import Header, HTTPException, Request
from fastapi_versioning import version

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.models_utils import to_proto
from nucliadb.models.internal.augment import Paragraph, ParagraphText
from nucliadb.search.api.v1.router import KB_PREFIX, api
from nucliadb.search.augmentor import augment_paragraphs
from nucliadb.search.search import cache, rerankers
from nucliadb.search.search.query_parser.parsers.retrieve import parse_retrieve
from nucliadb.search.search.query_parser.parsers.unit_retrieval import get_rephrased_query
from nucliadb.search.search.rerankers import RerankableItem, Reranker, RerankingOptions, get_reranker
from nucliadb.search.search.retrieval import text_block_search
from nucliadb_models.retrieval import (
    Metadata,
    RerankerScore,
    RetrievalMatch,
    RetrievalRequest,
    RetrievalResponse,
    Scores,
)
from nucliadb_models.search import NucliaDBClientType
from nucliadb_utils.utilities import get_audit


@api.post(
    f"/{KB_PREFIX}/{{kbid}}/retrieve",
    status_code=200,
    description="Search text blocks on a Knowledge Box",
    include_in_schema=False,
    tags=["Search"],
)
@version(1)
async def _retrieve_endpoint(
    request: Request,
    kbid: str,
    item: RetrievalRequest,
    x_ndb_client: NucliaDBClientType = Header(NucliaDBClientType.API),
    x_nucliadb_user: str = Header(""),
    x_forwarded_for: str = Header(""),
) -> RetrievalResponse:
    return await retrieve_endpoint(
        kbid,
        item,
        x_ndb_client=x_ndb_client,
        x_nucliadb_user=x_nucliadb_user,
        x_forwarded_for=x_forwarded_for,
    )


async def retrieve_endpoint(
    kbid: str,
    item: RetrievalRequest,
    *,
    x_ndb_client: NucliaDBClientType,
    x_nucliadb_user: str,
    x_forwarded_for: str,
) -> RetrievalResponse:
    audit = get_audit()
    start_time = time()

    try:
        parsed = await parse_retrieve(kbid, item)
    except InvalidQueryError as err:
        raise HTTPException(
            status_code=422,
            detail=str(err),
        )

    text_blocks, _, _, _ = await text_block_search(kbid, parsed.retrieval)

    if (
        parsed.retrieval.reranker is not None
        # we can only rerank correctly if we have a textual query to compare the
        # results with, otherwise the reranker may lead to incorrect behavior
        and parsed.retrieval.query.keyword is not None
        and len(parsed.retrieval.query.keyword.query) > 0
    ):
        reranker = get_reranker(parsed.retrieval.reranker)

        if isinstance(reranker, rerankers.NoopReranker):
            pass
        else:
            window = reranker.window or parsed.retrieval.top_k
            # cut and keep, at most, the number of elements needed to rerank
            text_blocks = text_blocks[:window]

            # use the rephrased query if available; we assume it'll be better for the reranker model
            reranker_query = get_rephrased_query(parsed) or parsed.retrieval.query.keyword.query

            with cache.request_caches():
                reranked = await rerank(
                    kbid, text_blocks, reranker, RerankingOptions(kbid=kbid, query=reranker_query)
                )
            text_blocks = reranked

    # cut the top K, we may have more due to extra results used for rank fusion or reranking
    text_blocks = text_blocks[: parsed.retrieval.top_k]

    # convert to response models
    matches = [text_block_match_to_retrieval_match(text_block) for text_block in text_blocks]

    if audit is not None:
        audit.retrieve(
            kbid,
            x_nucliadb_user,
            to_proto.client_type(x_ndb_client),
            x_forwarded_for,
            retrieval_time=time() - start_time,
        )

    return RetrievalResponse(matches=matches)


async def rerank(
    kbid: str, text_blocks: list[TextBlockMatch], reranker: Reranker, options: RerankingOptions
) -> list[TextBlockMatch]:
    text_blocks_by_id = {}  # used for faster lookup after rerank
    to_augment = []
    for text_block in text_blocks:
        text_blocks_by_id[text_block.paragraph_id.full()] = text_block
        to_augment.append(Paragraph.from_text_block_match(text_block))

    augmented_paragraphs = await augment_paragraphs(
        kbid, given=to_augment, select=[ParagraphText()], concurrency_control=asyncio.Semaphore(50)
    )

    to_rerank = []
    for text_block in text_blocks:
        augmented = augmented_paragraphs.get(text_block.paragraph_id, None)
        if augmented is not None and augmented.text is not None:
            # update text blocks with their augmented text
            text_block.text = augmented.text

        to_rerank.append(
            RerankableItem(
                id=text_block.paragraph_id.full(),
                score=text_block.score,
                score_type=text_block.score_type,
                content=text_block.text or "",  # TODO: add a warning, this shouldn't usually happen
            )
        )

    reranked = await reranker.rerank(to_rerank, options)

    matches = []
    for item in reranked:
        paragraph_id = item.id
        score = item.score
        score_type = item.score_type

        text_block = text_blocks_by_id[paragraph_id]
        text_block.scores.append(RerankerScore(score=score))
        text_block.score_type = score_type

        matches.append((paragraph_id, score))

    matches.sort(key=lambda x: x[1], reverse=True)

    reranked_text_blocks = []
    for order, (paragraph_id, _) in enumerate(matches):
        text_block = text_blocks_by_id[paragraph_id]
        text_block.order = order
        reranked_text_blocks.append(text_block)

    return reranked_text_blocks


def text_block_match_to_retrieval_match(item: TextBlockMatch) -> RetrievalMatch:
    return RetrievalMatch(
        id=item.paragraph_id.full(),
        score=Scores(
            value=item.current_score.score,
            source=item.current_score.source,
            type=item.current_score.type,
            history=item.scores,
        ),
        metadata=Metadata(
            field_labels=item.field_labels,
            paragraph_labels=item.paragraph_labels,
            is_an_image=item.is_an_image,
            is_a_table=item.is_a_table,
            source_file=item.representation_file,
            page=item.position.page_number,
            in_page_with_visual=item.page_with_visual,
        ),
    )
