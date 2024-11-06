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
from abc import ABC, abstractmethod
from typing import Iterable

from nucliadb.common.external_index_providers.base import TextBlockMatch
from nucliadb.common.ids import ParagraphId


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
    occurring deep in retrieved lists using a reciprocal distribution

    RRF = Σ(r ∈ R) 1 / (k + r(d))

    where:
    - d is a document
    - R is the set of retrievers
    - k (constant)
    - r(d) rank of document d in reranker r

    """

    # TODO: implement rank window
    def __init__(self, k: float = 60.0):
        # Constant used in RRF, studies agree on 60 as a good default value
        # giving good results across many datasets. k allow bigger score
        # difference among the best results and a smaller score difference among
        # bad results
        self.k = k

    def fuse(
        self, keyword: Iterable[TextBlockMatch], semantic: Iterable[TextBlockMatch]
    ) -> list[TextBlockMatch]:
        scores: dict[ParagraphId, float] = {}
        match_positions: dict[ParagraphId, list[tuple[int, int]]] = {}

        # sort results by it's score before merging them
        keyword = [k for k in sorted(keyword, key=lambda r: r.score, reverse=True)]
        semantic = [s for s in sorted(semantic, key=lambda r: r.score, reverse=True)]

        rankings = [keyword, semantic]
        for r, ranking in enumerate(rankings):
            for i, result in enumerate(ranking):
                id = result.paragraph_id
                scores.setdefault(id, 0)
                scores[id] += 1 / (self.k + i)

                position = (r, i)
                match_positions.setdefault(result.paragraph_id, []).append(position)

        merged = []
        for paragraph_id, positions in match_positions.items():
            for r, i in positions:
                score = scores[paragraph_id]
                result = rankings[r][i]
                result.score = score
                # TODO: should we change the score type?
                # result.score_type = SCORE_TYPE.RRF

                # We are appending multi-matches. Should we merge them?
                merged.append(result)

        merged.sort(key=lambda x: x.score, reverse=True)
        return merged
