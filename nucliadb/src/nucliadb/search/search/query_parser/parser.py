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


from pydantic import ValidationError

from nucliadb.search.search.query_parser.exceptions import ParserError
from nucliadb.search.search.query_parser.models import (
    LegacyRankFusion,
    MultiMatchBoosterReranker,
    NoopReranker,
    PredictReranker,
    RankFusion,
    ReciprocalRankFusion,
    Reranker,
    UnitRetrieval,
)
from nucliadb_models import search as search_models
from nucliadb_models.search import FindRequest


def parse_find(item: FindRequest) -> UnitRetrieval:
    parser = _FindParser(item)
    return parser.parse()


class _FindParser:
    def __init__(self, item: FindRequest):
        self.item = item

    def parse(self) -> UnitRetrieval:
        top_k = self._parse_top_k()
        try:
            rank_fusion = self._parse_rank_fusion()
        except ValidationError as exc:
            raise ParserError(f"Parsing error in rank fusion: {str(exc)}") from exc
        try:
            reranker = self._parse_reranker()
        except ValidationError as exc:
            raise ParserError(f"Parsing error in reranker: {str(exc)}") from exc

        # Adjust retrieval windows. Our current implementation assume:
        # `top_k <= reranker.window <= rank_fusion.window`
        # and as rank fusion is done before reranking, we must ensure rank
        # fusion window is at least, the reranker window
        if isinstance(reranker, PredictReranker):
            rank_fusion.window = max(rank_fusion.window, reranker.window)

        return UnitRetrieval(
            top_k=top_k,
            rank_fusion=rank_fusion,
            reranker=reranker,
        )

    def _parse_top_k(self) -> int:
        # while pagination is still there, FindRequest has a validator that converts
        # top_k to page_number and page_size. To get top_k, we can compute it from
        # those
        top_k = (self.item.page_number + 1) * self.item.page_size
        return top_k

    def _parse_rank_fusion(self) -> RankFusion:
        rank_fusion: RankFusion

        top_k = self._parse_top_k()
        window = min(top_k, 500)

        if isinstance(self.item.rank_fusion, search_models.RankFusionName):
            if self.item.rank_fusion == search_models.RankFusionName.LEGACY:
                rank_fusion = LegacyRankFusion(window=window)
            elif self.item.rank_fusion == search_models.RankFusionName.RECIPROCAL_RANK_FUSION:
                rank_fusion = ReciprocalRankFusion(window=window)
            else:
                raise ParserError(f"Unknown rank fusion algorithm: {self.item.rank_fusion}")

        elif isinstance(self.item.rank_fusion, search_models.LegacyRankFusion):
            rank_fusion = LegacyRankFusion(window=window)

        elif isinstance(self.item.rank_fusion, search_models.ReciprocalRankFusion):
            user_window = self.item.rank_fusion.window
            rank_fusion = ReciprocalRankFusion(
                k=self.item.rank_fusion.k,
                boosting=self.item.rank_fusion.boosting,
                window=min(max(user_window or 0, top_k), 500),
            )

        else:
            raise ParserError(f"Unknown rank fusion {self.item.rank_fusion}")

        return rank_fusion

    def _parse_reranker(self) -> Reranker:
        reranking: Reranker

        top_k = self._parse_top_k()

        if isinstance(self.item.reranker, search_models.RerankerName):
            if self.item.reranker == search_models.RerankerName.NOOP:
                reranking = NoopReranker()

            elif self.item.reranker == search_models.RerankerName.MULTI_MATCH_BOOSTER:
                reranking = MultiMatchBoosterReranker()

            elif self.item.reranker == search_models.RerankerName.PREDICT_RERANKER:
                # for predict rearnker, by default, we want a x2 factor with a
                # top of 200 results
                reranking = PredictReranker(window=min(top_k * 2, 200))

            else:
                raise ParserError(f"Unknown reranker algorithm: {self.item.reranker}")

        elif isinstance(self.item.reranker, search_models.PredictReranker):
            user_window = self.item.reranker.window
            reranking = PredictReranker(window=min(max(user_window or 0, top_k), 200))

        else:
            raise ParserError(f"Unknown reranker {self.item.reranker}")

        return reranking
