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
from typing import Iterable

from nucliadb.common.external_index_providers.base import TextBlockMatch
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

    def fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]:
        """Fuse keyword and semantic results and return a list with the merged
        results.

        """
        merged = self._fuse(keyword, semantic)
        return merged

    @abstractmethod
    def _fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]: ...


class LegacyRankFusion(RankFusionAlgorithm):
    """Legacy algorithm that given results from keyword and semantic search,
    mixes them in the following way:
    - 1st result from keyword search
    - 2nd result from semantic search
    - 2 keyword results and 1 semantic (and repeat)

    """

    @rank_fusion_observer.wrap({"type": "legacy"})
    def _fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]:
        merged: list[TextBlockMatch] = []

        # sort results by it's score before merging them
        keyword = [k for k in sorted(keyword, key=lambda r: r.score, reverse=True)]
        semantic = [s for s in sorted(semantic, key=lambda r: r.score, reverse=True)]

        for k in keyword:
            merged.append(k)

        nextpos = 1
        for s in semantic:
            merged.insert(nextpos, s)
            nextpos += 3

        return merged


class ReciprocalRankFusion(RankFusionAlgorithm):
    """Rank-based rank fusion algorithm. Discounts the weight of documents
    occurring deep in retrieved lists using a reciprocal distribution. It can be
    parametrized with weights to boost retrievers.

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
        keyword_weight: float = 1.0,
        semantic_weight: float = 1.0,
    ):
        super().__init__(window)
        # Constant used in RRF, studies agree on 60 as a good default value
        # giving good results across many datasets. k allow bigger score
        # difference among the best results and a smaller score difference among
        # bad results
        self._k = k
        self._keyword_boost = keyword_weight
        self._semantic_boost = semantic_weight

    @rank_fusion_observer.wrap({"type": "reciprocal_rank_fusion"})
    def _fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]:
        scores: dict[ParagraphId, tuple[float, SCORE_TYPE]] = {}
        match_positions: dict[ParagraphId, list[tuple[int, int]]] = {}

        # sort results by it's score before merging them
        keyword = [k for k in sorted(keyword, key=lambda r: r.score, reverse=True)]
        semantic = [s for s in sorted(semantic, key=lambda r: r.score, reverse=True)]

        rankings = [
            (keyword, self._keyword_boost),
            (semantic, self._semantic_boost),
        ]
        for r, (ranking, boost) in enumerate(rankings):
            for i, result in enumerate(ranking):
                id = result.paragraph_id
                score, score_type = scores.setdefault(id, (0, result.score_type))
                score += 1 / (self._k + i) * boost
                if {score_type, result.score_type} == {SCORE_TYPE.BM25, SCORE_TYPE.VECTOR}:
                    score_type = SCORE_TYPE.BOTH
                scores[id] = (score, score_type)

                position = (r, i)
                match_positions.setdefault(result.paragraph_id, []).append(position)

        merged = []
        for paragraph_id, positions in match_positions.items():
            # we are getting only one position, effectively deduplicating
            # multiple matches for the same text block
            r, i = match_positions[paragraph_id][0]
            score, score_type = scores[paragraph_id]
            result = rankings[r][0][i]
            result.score = score
            result.score_type = score_type
            merged.append(result)

        merged.sort(key=lambda x: x.score, reverse=True)
        return merged


def get_rank_fusion(rank_fusion: parser_models.RankFusion) -> RankFusionAlgorithm:
    """Given a rank fusion API type, return the appropiate rank fusion algorithm instance"""
    algorithm: RankFusionAlgorithm
    window = rank_fusion.window

    if isinstance(rank_fusion, parser_models.ReciprocalRankFusion):
        algorithm = ReciprocalRankFusion(
            k=rank_fusion.k,
            window=window,
            keyword_weight=rank_fusion.boosting.keyword,
            semantic_weight=rank_fusion.boosting.semantic,
        )

    else:
        logger.error(f"Unknown rank fusion algorithm {type(rank_fusion)}: {rank_fusion}. Using default")
        algorithm = ReciprocalRankFusion(window=window)

    return algorithm
