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
from typing import Optional
from unittest.mock import AsyncMock, patch

import pytest
from nidx_protos.nodereader_pb2 import SearchRequest

from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.search.predict import PredictEngine
from nucliadb.search.search.query import (
    apply_entities_filter,
    check_supported_filters,
)
from nucliadb.search.search.query_parser.fetcher import Fetcher
from nucliadb.search.search.query_parser.parsers.common import parse_semantic_query, query_with_synonyms
from nucliadb.tests.vectors import Q
from nucliadb_models.search import FindOptions, FindRequest
from nucliadb_protos.knowledgebox_pb2 import Synonyms
from nucliadb_protos.utils_pb2 import RelationNode


def test_parse_entities_to_filters():
    detected_entities = [
        RelationNode(value="John", ntype=RelationNode.NodeType.ENTITY, subtype="person")
    ]

    request = SearchRequest()
    request.field_filter.facet.facet = "/e/person/Austin"
    assert apply_entities_filter(request, detected_entities) == ["/e/person/John"]
    assert [x.facet.facet for x in request.field_filter.bool_and.operands] == [
        "/e/person/Austin",
        "/e/person/John",
    ]


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
        vectorset: Optional[str],
        matryoshka_dimension: Optional[int],
        expected_vectorset: Optional[str],
        expected_dimension: Optional[int],
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
                "nucliadb.search.search.query_parser.fetcher.get_matryoshka_dimension_cached",
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
