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
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Optional, TypeVar

from nucliadb.common.external_index_providers.base import ScoredTextBlock
from nucliadb.common.ids import ParagraphId
from nucliadb.search.search.query_parser import models as parser_models
from nucliadb_models.search import SCORE_TYPE
from nucliadb_telemetry.metrics import Observer

logger = logging.getLogger(__name__)

rank_fusion_observer = Observer(
    "rank_fusion",
    labels={"type": ""},
    buckets=[
        0.001,
        0.0025,
        0.005,
        0.01,
        0.025,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
    ],
)

ScoredItem = TypeVar("ScoredItem", bound=ScoredTextBlock)


class IndexSource(str, Enum):
    KEYWORD = auto()
    SEMANTIC = auto()
    GRAPH = auto()


class RankFusionAlgorithm(ABC):
    def __init__(self, window: int):
        self._window = window

    @property
    def window(self) -> int:
        """Phony number used to compute the number of elements to retrieve and
        feed the rank fusion algorithm.

        This is here for convinience, but a query plan should be the way to go.

        """
        return self._window

    def fuse(self, sources: dict[str, list[ScoredItem]]) -> list[ScoredItem]:
        """Fuse elements from multiple sources and return a list of merged
        results.

        If only one source is provided, rank fusion will be skipped.

        """
        sources_with_results = [x for x in sources.values() if len(x) > 0]
        if len(sources_with_results) == 1:
            # skip rank fusion, we only have a source
            merged = sources_with_results[0]
        else:
            merged = self._fuse(sources)

        # sort and return the unordered results from the implementation
        merged.sort(key=lambda r: r.score, reverse=True)
        return merged

    @abstractmethod
    def _fuse(self, sources: dict[str, list[ScoredItem]]) -> list[ScoredItem]:
        """Rank fusion implementation.

        Each concrete subclass must provide an implementation that merges
        `sources`, a group of unordered matches, into a list of unordered
        results with the new rank fusion score.

        Results can be deduplicated or changed by the rank fusion algorithm.

        """
        ...


class ReciprocalRankFusion(RankFusionAlgorithm):
    """Rank-based rank fusion algorithm. Discounts the weight of documents
    occurring deep in retrieved lists using a reciprocal distribution.

    This implementation can be further parametrized with a weight (boost) per
    retriever that will be applied to all documents ranked by it.

    RRF = Σ(r ∈ R) (1 / (k + r(d)) · w(r))

    where:
    - d is a document
    - R is the set of retrievers
    - k (constant)
    - r(d) rank of document d in reranker r
    - w(r) weight (boost) for retriever r

    RRF boosts matches from multiple retrievers and deduplicate them

    """

    def __init__(
        self,
        k: float = 60.0,
        *,
        window: int,
        weights: Optional[dict[str, float]] = None,
        default_weight: float = 1.0,
    ):
        super().__init__(window)
        # Constant used in RRF, studies agree on 60 as a good default value
        # giving good results across many datasets. k allow bigger score
        # difference among the best results and a smaller score difference among
        # bad results
        self._k = k
        self._weights = weights or {}
        self._default_weight = default_weight

    @rank_fusion_observer.wrap({"type": "reciprocal_rank_fusion"})
    def _fuse(
        self,
        sources: dict[str, list[ScoredItem]],
    ) -> list[ScoredItem]:
        # accumulated scores per paragraph
        scores: dict[ParagraphId, tuple[float, SCORE_TYPE]] = {}
        # pointers from paragraph to the original source
        match_positions: dict[ParagraphId, list[tuple[int, int]]] = {}

        # sort results by it's score before fusing them, as we need the rank
        sources = {
            retriever: sorted(values, key=lambda r: r.score, reverse=True)
            for retriever, values in sources.items()
        }
        rankings = [
            (values, self._weights.get(source, self._default_weight))
            for source, values in sources.items()
        ]
        for i, (ranking, weight) in enumerate(rankings):
            for rank, item in enumerate(ranking):
                id = item.paragraph_id
                score, score_type = scores.setdefault(id, (0, item.score_type))
                score += 1 / (self._k + rank) * weight
                if {score_type, item.score_type} == {SCORE_TYPE.BM25, SCORE_TYPE.VECTOR}:
                    score_type = SCORE_TYPE.BOTH
                scores[id] = (score, score_type)

                position = (i, rank)
                match_positions.setdefault(item.paragraph_id, []).append(position)

        merged = []
        for paragraph_id, positions in match_positions.items():
            # we are getting only one position, effectively deduplicating
            # multiple matches for the same text block
            i, j = match_positions[paragraph_id][0]
            score, score_type = scores[paragraph_id]
            item = rankings[i][0][j]
            item.score = score
            item.score_type = score_type
            merged.append(item)

        return merged


class WeightedCombSum(RankFusionAlgorithm):
    """Score-based rank fusion algorithm. Multiply each score by a list-specific
    weight (boost). Then adds the retrieval score of documents contained in more
    than one list and sort by score.

    wCombSUM = Σ(r ∈ R) (w(r) · S(r, d))

    where:
    - d is a document
    - R is the set of retrievers
    - w(r) weight (boost) for retriever r
    - S(r, d) is the score of document d given by retriever r

    wCombSUM boosts matches from multiple retrievers and deduplicate them. As a
    score ranking algorithm, comparison of different scores may lead to bad
    results.

    """

    def __init__(
        self,
        *,
        window: int,
        weights: Optional[dict[str, float]] = None,
        default_weight: float = 1.0,
    ):
        super().__init__(window)
        self._weights = weights or {}
        self._default_weight = default_weight

    @rank_fusion_observer.wrap({"type": "weighted_comb_sum"})
    def _fuse(self, sources: dict[str, list[ScoredItem]]) -> list[ScoredItem]:
        # accumulated scores per paragraph
        scores: dict[ParagraphId, tuple[float, SCORE_TYPE]] = {}
        # pointers from paragraph to the original source
        match_positions: dict[ParagraphId, list[tuple[int, int]]] = {}

        rankings = [
            (values, self._weights.get(source, self._default_weight))
            for source, values in sources.items()
        ]
        for i, (ranking, weight) in enumerate(rankings):
            for j, item in enumerate(ranking):
                id = item.paragraph_id
                score, score_type = scores.setdefault(id, (0, item.score_type))
                score += item.score * weight
                if {score_type, item.score_type} == {SCORE_TYPE.BM25, SCORE_TYPE.VECTOR}:
                    score_type = SCORE_TYPE.BOTH
                scores[id] = (score, score_type)

                position = (i, j)
                match_positions.setdefault(item.paragraph_id, []).append(position)

        merged = []
        for paragraph_id, positions in match_positions.items():
            # we are getting only one position, effectively deduplicating
            # multiple matches for the same text block
            i, j = match_positions[paragraph_id][0]
            score, score_type = scores[paragraph_id]
            item = rankings[i][0][j]
            item.score = score
            item.score_type = score_type
            merged.append(item)

        return merged


def get_rank_fusion(rank_fusion: parser_models.RankFusion) -> RankFusionAlgorithm:
    """Given a rank fusion API type, return the appropiate rank fusion algorithm instance"""
    algorithm: RankFusionAlgorithm
    window = rank_fusion.window

    if isinstance(rank_fusion, parser_models.ReciprocalRankFusion):
        algorithm = ReciprocalRankFusion(
            k=rank_fusion.k,
            window=window,
            weights={
                IndexSource.KEYWORD: rank_fusion.boosting.keyword,
                IndexSource.SEMANTIC: rank_fusion.boosting.semantic,
                IndexSource.GRAPH: rank_fusion.boosting.graph,
            },
        )

    else:
        logger.error(f"Unknown rank fusion algorithm {type(rank_fusion)}: {rank_fusion}. Using default")
        algorithm = ReciprocalRankFusion(window=window)

    return algorithm
