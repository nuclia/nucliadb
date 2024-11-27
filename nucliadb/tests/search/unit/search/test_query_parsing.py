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


from typing import Union

import pytest
from pydantic import ValidationError

from nucliadb.search.search.query_parser import models as parser_models
from nucliadb.search.search.query_parser.parser import parse_find
from nucliadb_models import search as search_models
from nucliadb_models.search import FindRequest


def test_find_query_parsing__top_k():
    find = FindRequest(
        top_k=20,
    )
    parsed = parse_find(find)
    assert parsed.top_k == find.top_k


@pytest.mark.parametrize(
    "rank_fusion,expected",
    [
        (search_models.RankFusionName.LEGACY, parser_models.LegacyRankFusion(window=20)),
        (
            search_models.RankFusionName.RECIPROCAL_RANK_FUSION,
            parser_models.ReciprocalRankFusion(window=20),
        ),
        (search_models.ReciprocalRankFusion(k=20), parser_models.ReciprocalRankFusion(window=20, k=20)),
        (
            search_models.ReciprocalRankFusion(
                boosting=search_models.ReciprocalRankFusionWeights(semantic=2, keyword=0.5)
            ),
            parser_models.ReciprocalRankFusion(
                window=20, boosting=search_models.ReciprocalRankFusionWeights(semantic=2, keyword=0.5)
            ),
        ),
        (search_models.ReciprocalRankFusion(window=50), parser_models.ReciprocalRankFusion(window=50)),
    ],
)
def test_find_query_parsing__rank_fusion(
    rank_fusion: Union[search_models.RankFusionName, search_models.RankFusion],
    expected: parser_models.RankFusion,
):
    find = FindRequest(
        top_k=20,
        rank_fusion=rank_fusion,
    )
    parsed = parse_find(find)
    assert parsed.rank_fusion == expected


# ATENTION: if you're changing this test, make sure public models, private
# models and parsing are change accordingly!
def test_find_query_parsing__rank_fusion_limits():
    FindRequest.model_validate(
        {
            "rank_fusion": {
                "name": "rrf",
                "window": 500,
            }
        }
    )

    parse_find(FindRequest(rank_fusion=search_models.ReciprocalRankFusion.model_construct(window=500)))

    with pytest.raises(ValidationError):
        FindRequest.model_validate(
            {
                "rank_fusion": {
                    "name": "rrf",
                    "window": 501,
                }
            }
        )

    parsed = parse_find(
        FindRequest(rank_fusion=search_models.ReciprocalRankFusion.model_construct(window=501))
    )
    assert parsed.rank_fusion.window == 500


@pytest.mark.parametrize(
    "reranker,expected",
    [
        (search_models.RerankerName.NOOP, parser_models.NoopReranker()),
        (search_models.RerankerName.MULTI_MATCH_BOOSTER, parser_models.MultiMatchBoosterReranker()),
        (search_models.RerankerName.PREDICT_RERANKER, parser_models.PredictReranker(window=20 * 2)),
        (search_models.PredictReranker(window=50), parser_models.PredictReranker(window=50)),
    ],
)
def test_find_query_parsing__reranker(
    reranker: Union[search_models.RerankerName, search_models.Reranker], expected: parser_models.Reranker
):
    find = FindRequest(
        top_k=20,
        reranker=reranker,
    )
    parsed = parse_find(find)
    assert parsed.reranker == expected


# ATENTION: if you're changing this test, make sure public models, private
# models and parsing are change accordingly!
def test_find_query_parsing__reranker_limits():
    FindRequest.model_validate(
        {
            "reranker": {
                "name": "predict",
                "window": 200,
            }
        }
    )

    parse_find(FindRequest(reranker=search_models.PredictReranker(window=200)))

    with pytest.raises(ValidationError):
        FindRequest.model_validate({"reranker": {"name": "predict", "window": 201}})

    parsed = parse_find(FindRequest(reranker=search_models.PredictReranker.model_construct(window=201)))
    assert parsed.reranker.window == 200
