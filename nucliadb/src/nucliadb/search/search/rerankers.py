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

from nucliadb.search.predict import ProxiedPredictAPIError, SendToPredictError
from nucliadb.search.utilities import get_predict
from nucliadb_models import search as search_models
from nucliadb_models.internal.predict import RerankModel
from nucliadb_models.search import (
    SCORE_TYPE,
    KnowledgeboxFindResults,
)
from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)

reranker_observer = Observer("reranker", labels={"type": ""})


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


@dataclass
class RerankingOptions:
    kbid: str

    # Query used to retrieve the results to be reranked. Smart rerankers will use it
    query: str

    # Number of results requested by the user (can be different from the amount
    # of retrieval results)
    top_k: int


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
    async def rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        """Given a query and a set of resources, rerank elements and return the
        list of reranked items sorted by decreasing score.

        """
        ...


class NoopReranker(Reranker):
    """No-operation reranker. Given a list of items to rerank, it does nothing
    with them and return the items in the same order. It can be use to not alter
    the previous ordering.
    """

    @property
    def needs_extra_results(self) -> bool:
        return False

    def items_needed(self, requested: int, shards: int = 1) -> int:
        return requested

    @reranker_observer.wrap({"type": "noop"})
    async def rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        return [
            RankedItem(
                id=item.id,
                score=item.score,
                score_type=item.score_type,
            )
            for item in items
        ]


class PredictReranker(Reranker):
    @property
    def needs_extra_results(self) -> bool:
        return True

    def items_needed(self, requested: int, shards: int = 1) -> int:
        # we want a x5 factor with a top of 500 results
        reranker_requested = min(requested * shards * 5, 500)
        actual_requested = requested * shards
        return math.ceil(max(reranker_requested, actual_requested) / shards)

    @reranker_observer.wrap({"type": "predict"})
    async def rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        if len(items) == 0:
            return []

        predict = get_predict()

        # Conversion to format expected by predict. At the same time,
        # deduplicates paragraphs found in different indices
        context = {item.id: item.content for item in items}
        request = RerankModel(
            question=options.query,
            user_id="",  # TODO
            context=context,
        )
        try:
            response = await predict.rerank(options.kbid, request)
        except (SendToPredictError, ProxiedPredictAPIError):
            # predict failed, we can't rerank
            reranked = [
                RankedItem(
                    id=item.id,
                    score=item.score,
                    score_type=item.score_type,
                )
                for item in items
            ]
        else:
            reranked = [
                RankedItem(
                    id=id,
                    score=score,
                    score_type=SCORE_TYPE.RERANKER,
                )
                for id, score in response.context_scores.items()
            ]
        sort_by_score(reranked)
        best = reranked[: options.top_k]
        return best


class MultiMatchBoosterReranker(Reranker):
    """This reranker gives more value to items that come from different indices"""

    @property
    def needs_extra_results(self) -> bool:
        return False

    def items_needed(self, requested: int, shards: int = 1) -> int:
        return requested

    @reranker_observer.wrap({"type": "multi_match_booster"})
    async def rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        """Given a list of rerankable items, boost matches that appear multiple
        times. The returned list can be smaller than the initial, as repeated
        matches are deduplicated.
        """
        reranked_by_id = {}
        for item in items:
            if item.id not in reranked_by_id:
                reranked_by_id[item.id] = RankedItem(
                    id=item.id,
                    score=item.score,
                    score_type=item.score_type,
                )
            else:
                # it's a mutiple match, boost the score
                if reranked_by_id[item.id].score < item.score:
                    # previous implementation noted that we are using vector
                    # score x2 when we find a multiple match. However, this may
                    # not be true, as the same paragraph could come in any
                    # position in the rank fusioned result list
                    reranked_by_id[item.id].score = item.score * 2

                reranked_by_id[item.id].score_type = SCORE_TYPE.BOTH

        reranked = list(reranked_by_id.values())
        sort_by_score(reranked)
        return reranked


def get_default_reranker() -> Reranker:
    return MultiMatchBoosterReranker()


def get_reranker(kind: search_models.Reranker) -> Reranker:
    reranker: Reranker
    if kind == search_models.Reranker.PREDICT_RERANKER:
        reranker = PredictReranker()
    elif kind == search_models.Reranker.MULTI_MATCH_BOOSTER:
        reranker = MultiMatchBoosterReranker()
    elif kind == search_models.Reranker.NOOP:
        reranker = NoopReranker()
    else:
        logger.warning(f"Unknown reranker requested: {kind}. Using default instead")
        reranker = get_default_reranker()
    return reranker


def sort_by_score(items: list[RankedItem]):
    """Sort `items` in place by decreasing score"""
    items.sort(key=lambda item: item.score, reverse=True)


def apply_reranking(results: KnowledgeboxFindResults, reranked: list[RankedItem]):
    """Given a list of reranked items, update the find results payload.

    *ATENTION* we assume `reranked` is an ordered list of decreasing relevance
    and contains *only* the items relevant for this response. Any paragraph not
    found in `reranked` will be removed from the `results`

    """
    inverted_results = {}
    for rid, resource in results.resources.items():
        for field_id, field in resource.fields.items():
            for paragraph_id, paragraph in field.paragraphs.items():
                inverted_results[paragraph_id] = (
                    paragraph,
                    (field_id, field),
                    (rid, resource),
                )

    # update results and best matches according to new scores
    results.best_matches.clear()
    for order, item in enumerate(reranked):
        paragraph_id = item.id
        paragraph = inverted_results[paragraph_id][0]
        paragraph.score = item.score
        paragraph.score_type = item.score_type
        paragraph.order = order
        results.best_matches.append(paragraph_id)

    # prune uneeded results (not appearing in `reranked`)
    extra = set(inverted_results.keys()) - set(results.best_matches)
    for paragraph_id in extra:
        _, (field_id, field), (rid, resource) = inverted_results[paragraph_id]
        field.paragraphs.pop(paragraph_id)
        if len(field.paragraphs) == 0:
            resource.fields.pop(field_id)

        if len(resource.fields) == 0:
            results.resources.pop(rid)
