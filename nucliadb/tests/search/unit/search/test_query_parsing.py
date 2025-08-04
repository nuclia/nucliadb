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
from unittest.mock import patch

import pytest
from pydantic import ValidationError

from nucliadb.common.maindb.driver import Driver
from nucliadb.search.predict import DummyPredictEngine
from nucliadb.search.search.query_parser import models as parser_models
from nucliadb.search.search.query_parser.parsers import parse_find
from nucliadb.search.search.query_parser.parsers.find import fetcher_for_find
from nucliadb.search.utilities import PredictEngine, get_predict
from nucliadb_models import search as search_models
from nucliadb_models.search import FindRequest


@pytest.fixture(scope="function", autouse=True)
def disable_hidden_resources_check():
    with patch(
        "nucliadb.search.search.query_parser.parsers.find.filter_hidden_resources", return_value=False
    ):
        yield


async def test_find_query_parsing__top_k(
    dummy_predict: PredictEngine,
):
    find = FindRequest(
        top_k=20,
    )
    parsed = await parse_find("kbid", find)
    assert parsed.retrieval.top_k == find.top_k


@pytest.mark.parametrize(
    "rank_fusion,expected",
    [
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
async def test_find_query_parsing__rank_fusion(
    rank_fusion: Union[search_models.RankFusionName, search_models.RankFusion],
    expected: parser_models.RankFusion,
    dummy_predict: PredictEngine,
):
    find = FindRequest(
        top_k=20,
        rank_fusion=rank_fusion,
        reranker=search_models.RerankerName.NOOP,
    )
    parsed = await parse_find("kbid", find)
    assert parsed.retrieval.rank_fusion is not None
    assert parsed.retrieval.rank_fusion == expected


# ATENTION: if you're changing this test, make sure public models, private
# models and parsing are change accordingly!
async def test_find_query_parsing__rank_fusion_limits(dummy_predict: PredictEngine):
    FindRequest.model_validate(
        {
            "rank_fusion": {
                "name": "rrf",
                "window": 500,
            }
        }
    )

    await parse_find(
        "kbid", FindRequest(rank_fusion=search_models.ReciprocalRankFusion.model_construct(window=500))
    )

    with pytest.raises(ValidationError):
        FindRequest.model_validate(
            {
                "rank_fusion": {
                    "name": "rrf",
                    "window": 501,
                }
            }
        )

    parsed = await parse_find(
        "kbid", FindRequest(rank_fusion=search_models.ReciprocalRankFusion.model_construct(window=501))
    )
    assert parsed.retrieval.rank_fusion is not None and parsed.retrieval.rank_fusion.window == 500


@pytest.mark.parametrize(
    "reranker,expected",
    [
        (search_models.RerankerName.NOOP, parser_models.NoopReranker()),
        (search_models.RerankerName.PREDICT_RERANKER, parser_models.PredictReranker(window=20 * 2)),
        (search_models.PredictReranker(window=50), parser_models.PredictReranker(window=50)),
    ],
)
async def test_find_query_parsing__reranker(
    dummy_predict: PredictEngine,
    reranker: Union[search_models.RerankerName, search_models.Reranker],
    expected: parser_models.Reranker,
):
    find = FindRequest(
        top_k=20,
        reranker=reranker,
    )
    parsed = await parse_find("kbid", find)
    assert parsed.retrieval.reranker is not None
    assert parsed.retrieval.reranker == expected


# ATENTION: if you're changing this test, make sure public models, private
# models and parsing are change accordingly!
async def test_find_query_parsing__reranker_limits(dummy_predict: PredictEngine):
    FindRequest.model_validate(
        {
            "reranker": {
                "name": "predict",
                "window": 200,
            }
        }
    )

    await parse_find("kbid", FindRequest(reranker=search_models.PredictReranker(window=200)))

    with pytest.raises(ValidationError):
        FindRequest.model_validate({"reranker": {"name": "predict", "window": 201}})

    parsed = await parse_find(
        "kbid", FindRequest(reranker=search_models.PredictReranker.model_construct(window=201))
    )
    assert (
        isinstance(parsed.retrieval.reranker, parser_models.PredictReranker)
        and parsed.retrieval.reranker.window == 200
    )


async def test_find_query_parsing__query_image(maindb_driver: Driver, dummy_predict: PredictEngine):
    """We want to check that the rephrased query is used for keyword search if an image is provided."""
    predict = get_predict()
    assert isinstance(predict, DummyPredictEngine)

    find = FindRequest(
        query="whatever",
        query_image=search_models.Image(
            content_type="image/png",
            b64encoded="mybase64encodedimage==",
        ),
    )

    fetcher = fetcher_for_find("kbid", find)
    parsed = await parse_find("kbid", find, fetcher=fetcher)
    assert parsed.retrieval.query.keyword is not None
    assert parsed.retrieval.query.keyword.query == "<REPHRASED-QUERY>"

    # If rephrase is on but there is no query image, we should use the original query
    find.rephrase = True
    find.query_image = None
    fetcher = fetcher_for_find("kbid", find)
    parsed = await parse_find("kbid", find, fetcher=fetcher)
    assert parsed.retrieval.query.keyword is not None
    assert parsed.retrieval.query.keyword.query == find.query
