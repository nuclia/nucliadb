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

from typing import Union, cast

from nucliadb.search.search.query_parser.exceptions import ParserError
from nucliadb.search.search.query_parser.models import (
    MultiMatchBoosterReranker,
    NoopReranker,
    ParsedFindRequest,
    PredictReranker,
    RankFusion,
    Reranker,
)
from nucliadb_models import search as search_models
from nucliadb_models.search import FindRequest


def parse_find(item: FindRequest) -> ParsedFindRequest:
    parser = _FindParser(item)
    return parser.parse()


class _FindParser:
    def __init__(self, item: FindRequest):
        self.item = item

    def parse(self) -> ParsedFindRequest:
        top_k = self._parse_top_k()
        rank_fusion = self._parse_rank_fusion()
        reranker = self._parse_reranker()

        # Adjust retrieval windows. Our current implementation assume:
        # `top_k <= reranker.window <= rank_fusion.window`
        # and as rank fusion is done before reranking, we must ensure rank
        # fusion window is at least, the reranker window
        if isinstance(reranker, PredictReranker):
            reranker = cast(PredictReranker, reranker)
            rank_fusion.window = max(rank_fusion.window, reranker.window)

        return ParsedFindRequest(
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
        top_k = self._parse_top_k()

        rank_fusion_kind: Union[search_models.LegacyRankFusion, search_models.ReciprocalRankFusion]
        if isinstance(self.item.rank_fusion, search_models.RankFusionName):
            if self.item.rank_fusion == search_models.RankFusionName.LEGACY:
                rank_fusion_kind = search_models.LegacyRankFusion()
            elif self.item.rank_fusion == search_models.RankFusionName.RECIPROCAL_RANK_FUSION:
                rank_fusion_kind = search_models.ReciprocalRankFusion()
            else:
                raise ParserError(f"Unknown rank fusion algorithm: {self.item.rank_fusion}")
        else:
            rank_fusion_kind = self.item.rank_fusion

        return RankFusion(kind=rank_fusion_kind, window=top_k)

    def _parse_reranker(self) -> Reranker:
        top_k = self._parse_top_k()

        reranking: Reranker

        if self.item.reranker == search_models.Reranker.NOOP:
            reranking = NoopReranker()

        elif self.item.reranker == search_models.Reranker.MULTI_MATCH_BOOSTER:
            reranking = MultiMatchBoosterReranker()

        elif self.item.reranker == search_models.Reranker.PREDICT_RERANKER:
            # for predict rearnker, we want a x5 factor with a top of 500 results
            reranking = PredictReranker(window=min(top_k * 5, 500))

        else:
            raise ParserError(f"Unknown reranker {self.item.reranker}")

        return reranking
