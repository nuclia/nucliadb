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
from abc import ABC, abstractmethod, abstractproperty
from dataclasses import dataclass
from typing import Optional

from nucliadb.search.predict import ProxiedPredictAPIError, SendToPredictError
from nucliadb.search.search.query_parser import models as parser_models
from nucliadb.search.utilities import get_predict
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


class Reranker(ABC):
    @abstractproperty
    def window(self) -> Optional[int]:
        """Number of elements the reranker requests. `None` means no specific
        window is enforced."""
        ...

    @property
    def needs_extra_results(self) -> bool:
        return self.window is not None

    async def rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        """Given a query and a set of resources, rerank elements and return the
        list of reranked items sorted by decreasing score. The list will contain
        at most, `window` elements.

        """
        # Enforce reranker window and drop the rest
        # XXX: other search engines allow a mix of reranked and not reranked
        # results, there's no technical reason we can't do it
        items = items[: self.window]
        reranked = await self._rerank(items, options)
        return reranked

    @abstractmethod
    async def _rerank(
        self, items: list[RerankableItem], options: RerankingOptions
    ) -> list[RankedItem]: ...


class NoopReranker(Reranker):
    """No-operation reranker. Given a list of items to rerank, it does nothing
    with them and return the items in the same order. It can be use to not alter
    the previous ordering.

    """

    @property
    def window(self) -> Optional[int]:
        return None

    @reranker_observer.wrap({"type": "noop"})
    async def _rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
        return [
            RankedItem(
                id=item.id,
                score=item.score,
                score_type=item.score_type,
            )
            for item in items
        ]


class PredictReranker(Reranker):
    """Rerank using a reranking model.

    It uses Predict API to rerank elements using a model trained for this

    """

    def __init__(self, window: int):
        self._window = window

    @property
    def window(self) -> int:
        return self._window

    @reranker_observer.wrap({"type": "predict"})
    async def _rerank(self, items: list[RerankableItem], options: RerankingOptions) -> list[RankedItem]:
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
        best = reranked
        return best


def get_reranker(reranker: parser_models.Reranker) -> Reranker:
    algorithm: Reranker

    if isinstance(reranker, parser_models.NoopReranker):
        algorithm = NoopReranker()

    elif isinstance(reranker, parser_models.PredictReranker):
        algorithm = PredictReranker(reranker.window)

    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"

    return algorithm


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
