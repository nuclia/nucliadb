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

import logging
import math
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass

from nucliadb.search.utilities import get_predict
from nucliadb_models import search as search_models
from nucliadb_models.internal.predict import RerankModel
from nucliadb_models.search import (
    SCORE_TYPE,
    KnowledgeboxFindResults,
)

logger = logging.getLogger(__name__)


@dataclass
class RerankableItem:
    id: str
    score: float
    score_type: SCORE_TYPE
    content: str


@dataclass
class RankedItem:
    id: str
    score: float
    score_type: SCORE_TYPE


class Reranker(ABC):
    @abstractproperty
    def needs_extra_results(self) -> bool: ...

    @abstractmethod
    def items_needed(self, requested: int, shards: int = 1) -> int:
        """Return the number of retrieval results (per shard) the reranker
        needs.

        `requested` is the amount of results requested
        `shards`: for sharded index providers, this is the amount of shards that
        will be queried

        """
        ...

    @abstractmethod
    async def rerank(self, kbid: str, query: str, items: list[RerankableItem]) -> list[RankedItem]:
        """Given a query and a set of resources, rerank elements and return the
        new scores for each item by id"""
        ...

    async def rerank_find_response(self, kbid: str, query: str, response: KnowledgeboxFindResults):
        to_rerank = _get_items_to_rerank(response)
        reranked = await self.rerank(kbid, query, to_rerank)
        apply_reranking(response, reranked)


class PredictReranker(Reranker):
    @property
    def needs_extra_results(self) -> bool:
        return True

    def items_needed(self, requested: int, shards: int = 1) -> int:
        # we want a x5 factor with a top of 500 results
        reranker_requested = min(requested * shards * 5, 500)
        actual_requested = requested * shards
        return math.ceil(max(reranker_requested, actual_requested) / shards)

    async def rerank(self, kbid: str, query: str, items: list[RerankableItem]) -> list[RankedItem]:
        predict = get_predict()

        request = RerankModel(
            question=query,
            user_id="",  # TODO
            context={item.id: item.content for item in items},
        )
        response = await predict.rerank(kbid, request)

        reranked = []
        for id, score in response.context_scores.items():
            reranked.append(
                RankedItem(
                    id=id,
                    score=score,
                    score_type=SCORE_TYPE.RERANKER,
                )
            )
        return reranked


class MultiMatchBoosterReranker(Reranker):
    """This reranker gives more value to items that come from different indices"""

    @property
    def needs_extra_results(self) -> bool:
        return False

    def items_needed(self, requested: int, shards: int = 1) -> int:
        return requested

    async def rerank(self, kbid: str, query: str, items: list[RerankableItem]) -> list[RankedItem]:
        # TODO: not implemented yet, as the reranking code is coupled with
        # hydration and response composition in find_merge.py
        reranked = []
        for item in items:
            reranked.append(
                RankedItem(
                    id=item.id,
                    score=item.score,
                    score_type=item.score_type,
                )
            )
        return reranked


def get_reranker(kind: search_models.Reranker) -> Reranker:
    reranker: Reranker
    if kind == search_models.Reranker.PREDICT_RERANKER:
        reranker = PredictReranker()
    elif kind == search_models.Reranker.MULTI_MATCH_BOOSTER:
        reranker = MultiMatchBoosterReranker()
    else:
        logger.warning(f"Unknown reranker requested: {kind}. Using multi-match booster instead")
        reranker = MultiMatchBoosterReranker()
    return reranker


def apply_reranking(results: KnowledgeboxFindResults, reranked: list[RankedItem]):
    inverted_results = {}
    for rid, resource in results.resources.items():
        for field_id, field in resource.fields.items():
            for paragraph_id, paragraph in field.paragraphs.items():
                inverted_results[paragraph_id] = (
                    paragraph,
                    (field_id, field),
                    (rid, resource),
                )

    # order results by new score
    best_matches = []
    for item in reranked:
        paragraph = inverted_results[item.id][0]
        paragraph.score = item.score
        paragraph.score_type = item.score_type
        best_matches.append((item.id, item.score))

    best_matches.sort(key=lambda x: x[1], reverse=True)

    # update best matches according to new scores
    cut = results.page_size
    results.best_matches.clear()
    for order, (paragraph_id, _) in enumerate(best_matches[:cut]):
        paragraph = inverted_results[paragraph_id][0]
        paragraph.order = order
        results.best_matches.append(paragraph_id)

    # cut uneeded results
    extra = set(inverted_results.keys()) - set(results.best_matches)
    for paragraph_id in extra:
        _, (field_id, field), (rid, resource) = inverted_results[paragraph_id]
        field.paragraphs.pop(paragraph_id)
        if len(field.paragraphs) == 0:
            resource.fields.pop(field_id)

        if len(resource.fields) == 0:
            results.resources.pop(rid)


def _get_items_to_rerank(results: KnowledgeboxFindResults) -> list[RerankableItem]:
    return [
        RerankableItem(
            id=paragraph_id,
            score=paragraph.score,
            score_type=paragraph.score_type,
            content=paragraph.text,
        )
        for resource in results.resources.values()
        for field in resource.fields.values()
        for paragraph_id, paragraph in field.paragraphs.items()
    ]
