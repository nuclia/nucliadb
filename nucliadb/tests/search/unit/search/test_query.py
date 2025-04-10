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
from unittest.mock import AsyncMock, Mock, patch

import pytest

from nucliadb.search.predict import PredictEngine
from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.query import (
    QueryParser,
    apply_entities_filter,
    check_supported_filters,
)
from nucliadb.search.search.query_parser.old_filters import OldFilterParams
from nucliadb.tests.vectors import Q
from nucliadb_models.search import MinScore, SearchOptions
from nucliadb_protos.knowledgebox_pb2 import Synonyms
from nucliadb_protos.nodereader_pb2 import SearchRequest
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
    def query_parser(self, get_synonyms):
        qp = QueryParser(
            kbid="kbid",
            features=[],
            query="query",
            old_filters=OldFilterParams(label_filters=[], keyword_filters=[]),
            faceted=[],
            top_k=10,
            min_score=MinScore(semantic=0.5),
            with_synonyms=True,
        )
        with patch("nucliadb.search.search.query_parser.fetcher.get_kb_synonyms", get_synonyms):
            yield qp

    async def test_not_applies_if_empty_body(self, query_parser: QueryParser, get_synonyms):
        query_parser.query = ""
        search_request = Mock()
        await query_parser.parse_synonyms(search_request)

        get_synonyms.assert_not_awaited()
        search_request.ClearField.assert_not_called()

    async def test_not_applies_if_synonyms_object_not_found(
        self, query_parser: QueryParser, get_synonyms
    ):
        query_parser.query = "planet"
        get_synonyms.return_value = None
        request = Mock()

        await query_parser.parse_synonyms(Mock())

        request.ClearField.assert_not_called()
        get_synonyms.assert_awaited_once_with("kbid")

    async def test_not_applies_if_synonyms_not_found_for_query(
        self, query_parser: QueryParser, get_synonyms
    ):
        query_parser.query = "foobar"
        request = Mock()

        await query_parser.parse_synonyms(request)

        request.ClearField.assert_not_called()

        # Append planetary at the end of the query to test that partial terms are not replaced
        query_parser.query = "which is this planet? planetary"
        await query_parser.parse_synonyms(request)

        request.ClearField.assert_called_once_with("body")
        assert request.advanced_query == "which is this (planet OR earth OR globe)? planetary"


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
        parser = QueryParser(
            kbid="kbid",
            features=[SearchOptions.SEMANTIC],
            vectorset=vectorset,
            # irrelevant mandatory args
            query="my query",
            old_filters=OldFilterParams(label_filters=[], keyword_filters=[]),
            top_k=20,
            min_score=MinScore(bm25=0, semantic=0),
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
            request, incomplete, _, _ = await parser.parse()
            assert not incomplete

            assert request.vectorset == expected_vectorset
            assert len(request.vector) == expected_dimension
