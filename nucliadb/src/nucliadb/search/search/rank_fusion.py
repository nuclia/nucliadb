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
from typing import Iterable, Union, cast

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId
from nucliadb_models import search as search_models
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
    @abstractmethod
    def fuse(
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
    def fuse(
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
        keyword_weight: float = 1.0,
        semantic_weight: float = 1.0,
    ):
        # Constant used in RRF, studies agree on 60 as a good default value
        # giving good results across many datasets. k allow bigger score
        # difference among the best results and a smaller score difference among
        # bad results
        self.k = k
        self.keyword_boost = keyword_weight
        self.semantic_boost = semantic_weight

    @rank_fusion_observer.wrap({"type": "reciprocal_rank_fusion"})
    def fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]:
        scores: dict[ParagraphId, float] = {}
        match_positions: dict[ParagraphId, list[tuple[int, int]]] = {}

        # sort results by it's score before merging them
        keyword = [k for k in sorted(keyword, key=lambda r: r.score, reverse=True)]
        semantic = [s for s in sorted(semantic, key=lambda r: r.score, reverse=True)]

        rankings = [
            (keyword, self.keyword_boost),
            (semantic, self.semantic_boost),
        ]
        for r, (ranking, boost) in enumerate(rankings):
            for i, result in enumerate(ranking):
                id = result.paragraph_id
                scores.setdefault(id, 0)
                scores[id] += 1 / (self.k + i) * boost

                position = (r, i)
                match_positions.setdefault(result.paragraph_id, []).append(position)

        merged = []
        for paragraph_id, positions in match_positions.items():
            # we are getting only one position, effectively deduplicating
            # multiple matches for the same text block
            r, i = match_positions[paragraph_id][0]
            score = scores[paragraph_id]
            result = rankings[r][0][i]
            result.score = score
            result.score_type = SCORE_TYPE.RANK_FUSION
            merged.append(result)

        merged.sort(key=lambda x: x.score, reverse=True)
        return merged


def get_default_rank_fusion() -> RankFusionAlgorithm:
    return LegacyRankFusion()


def get_rank_fusion(
    rf: Union[search_models.RankFusionName, search_models.RankFusion],
) -> RankFusionAlgorithm:
    """Given a rank fusion API type, return the appropiate rank fusion algorithm instance"""
    algorithm: RankFusionAlgorithm

    if isinstance(rf, search_models.LegacyRankFusion):
        rf = cast(search_models.LegacyRankFusion, rf)
        algorithm = LegacyRankFusion()

    elif isinstance(rf, search_models.ReciprocalRankFusion):
        rf = cast(search_models.ReciprocalRankFusion, rf)
        algorithm = ReciprocalRankFusion(
            k=rf.k,
            keyword_weight=rf.boosting.keyword,
            semantic_weight=rf.boosting.semantic,
        )

    elif rf == search_models.RankFusionName.LEGACY:
        algorithm = LegacyRankFusion()

    elif rf == search_models.RankFusionName.RECIPROCAL_RANK_FUSION:
        algorithm = ReciprocalRankFusion()

    else:
        logger.error(f"Unknown rank fusion algorithm {type(rf)} {rf}. Using default")
        algorithm = get_default_rank_fusion()

    return algorithm
