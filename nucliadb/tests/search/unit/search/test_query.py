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
import unittest
from unittest.mock import AsyncMock, patch

import pytest

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.search.predict import PredictEngine
from nucliadb.search.search.query import (
    check_supported_filters,
)
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.parsers.common import parse_semantic_query, query_with_synonyms
from nucliadb.tests.vectors import Q
from nucliadb_models.search import FindOptions, FindRequest
from nucliadb_protos.knowledgebox_pb2 import Synonyms


@pytest.fixture()
def kbdm(read_only_txn):
    kbdm = unittest.mock.AsyncMock()
    with (
        unittest.mock.patch(f"nucliadb.search.search.query.datamanagers.kb", kbdm),
        unittest.mock.patch(f"nucliadb.search.search.query_parser.fetcher.datamanagers.kb", kbdm),
    ):
        yield kbdm


class TestApplySynonymsToRequest:
    @pytest.fixture
    def get_synonyms(self):
        get_kb_synonyms = AsyncMock()
        synonyms = Synonyms()
        synonyms.terms["planet"].synonyms.extend(["earth", "globe"])
        get_kb_synonyms.return_value = synonyms
        yield get_kb_synonyms

    @pytest.fixture
    def fetcher(self, get_synonyms: AsyncMock):
        fetcher = Fetcher(
            "kbid",
            query="query",
            user_vector=None,
            vectorset=None,
            rephrase=False,
            rephrase_prompt=None,
            generative_model=None,
            query_image=None,
        )
        with patch("nucliadb.search.search.query_parser.fetcher.get_kb_synonyms", get_synonyms):
            yield fetcher

    async def test_not_applies_if_empty_body(self, fetcher: Fetcher, get_synonyms: AsyncMock):
        synonyms_query = await query_with_synonyms(query="", fetcher=fetcher)

        assert synonyms_query is None
        get_synonyms.assert_not_awaited()

    async def test_not_applies_if_synonyms_object_not_found(
        self, fetcher: Fetcher, get_synonyms: AsyncMock
    ):
        get_synonyms.return_value = None

        synonyms_query = await query_with_synonyms(query="planet", fetcher=fetcher)

        assert synonyms_query is None
        get_synonyms.assert_awaited_once_with("kbid")

    async def test_not_applies_if_synonyms_not_found_for_query(
        self, fetcher: Fetcher, get_synonyms: AsyncMock
    ):
        synonyms_query = await query_with_synonyms(query="foobar", fetcher=fetcher)

        assert synonyms_query is None
        get_synonyms.assert_awaited_once_with("kbid")

        # Append planetary at the end of the query to test that partial terms are not replaced
        synonyms_query = await query_with_synonyms(
            query="which is this planet? planetary", fetcher=fetcher
        )
        assert synonyms_query == "which is this (planet OR earth OR globe)? planetary"


def test_check_supported_filters():
    check_supported_filters({"literal": "a"}, ["a"])
    check_supported_filters({"or": [{"literal": "a"}, {"literal": "b"}]}, [])
    with pytest.raises(InvalidQueryError):
        check_supported_filters({"or": [{"literal": "a"}, {"literal": "b"}]}, ["b"])
    with pytest.raises(InvalidQueryError):
        check_supported_filters(
            {"and": [{"literal": "a"}, {"and": [{"literal": "c"}, {"literal": "b"}]}]},
            ["b"],
        )


class TestVectorSetAndMatryoshkaParsing:
    @pytest.mark.parametrize(
        "vectorset,matryoshka_dimension,expected_vectorset,expected_dimension",
        [
            (None, None, "<PREDICT-DEFAULT-SEMANTIC-MODEL>", len(Q)),
            (None, len(Q) - 20, "<PREDICT-DEFAULT-SEMANTIC-MODEL>", len(Q) - 20),
            ("vectorset_id", None, "vectorset_id", len(Q)),
            ("vectorset_id", len(Q) - 20, "vectorset_id", len(Q) - 20),
        ],
    )
    async def test_query_without_vectorset_nor_matryoshka(
        self,
        dummy_predict: PredictEngine,
        vectorset: str | None,
        matryoshka_dimension: int | None,
        expected_vectorset: str | None,
        expected_dimension: int | None,
    ):
        fetcher = Fetcher(
            "kbid",
            query="query",
            vectorset=vectorset,
            # irrelevant mandatory args
            user_vector=None,
            rephrase=False,
            rephrase_prompt=None,
            generative_model=None,
            query_image=None,
        )

        with (
            patch(
                "nucliadb.search.search.query_parser.fetcher.datamanagers.vectorsets.exists",
                new=AsyncMock(return_value=(vectorset is not None)),
            ),
            patch(
                "nucliadb.search.search.query_parser.fetcher.Fetcher.get_matryoshka_dimension_cached",
                new=AsyncMock(return_value=matryoshka_dimension),
            ),
            patch("nucliadb.common.datamanagers.utils.get_driver"),
            patch("nucliadb.common.datamanagers.vectorsets.get_kv_pb"),
        ):
            semantic_query = await parse_semantic_query(
                FindRequest(
                    features=[FindOptions.SEMANTIC],
                    vectorset=vectorset,
                ),
                fetcher=fetcher,
            )
            assert semantic_query.vectorset == expected_vectorset
            assert semantic_query.query is not None
            assert len(semantic_query.query) == expected_dimension
