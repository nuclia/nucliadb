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
from unittest.mock import AsyncMock, Mock, patch

import pytest
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata, Synonyms
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.utils_pb2 import RelationNode, VectorSimilarity

from nucliadb.search.search.exceptions import InvalidQueryError
from nucliadb.search.search.query import (
    QueryParser,
    check_supported_filters,
    get_default_semantic_min_score,
    get_kb_model_default_min_score,
    parse_entities_to_filters,
)
from nucliadb_models.search import MinScore

QUERY_MODULE = "nucliadb.search.search.query"


def test_parse_entities_to_filters():
    detected_entities = [
        RelationNode(value="John", ntype=RelationNode.NodeType.ENTITY, subtype="person")
    ]

    request = SearchRequest()
    assert parse_entities_to_filters(request, detected_entities) == ["/e/person/John"]
    assert request.filter.field_labels == ["/e/person/John"]

    assert parse_entities_to_filters(request, detected_entities) == []
    assert request.filter.field_labels == ["/e/person/John"]


@pytest.fixture()
def get_kb_model_default_min_score_mock():
    with unittest.mock.patch(f"{QUERY_MODULE}.get_kb_model_default_min_score") as mock:
        yield mock


async def test_get_default_semantic_min_score(get_kb_model_default_min_score_mock):
    get_kb_model_default_min_score_mock.return_value = 1.5

    assert await get_default_semantic_min_score("kbid") == 1.5

    get_default_semantic_min_score.cache_clear()


async def test_get_default_semantic_min_score_default_value(
    get_kb_model_default_min_score_mock,
):
    get_kb_model_default_min_score_mock.return_value = None

    assert await get_default_semantic_min_score("kbid") == 0.7

    get_default_semantic_min_score.cache_clear()


async def test_get_default_semantic_min_score_is_cached(
    get_kb_model_default_min_score_mock,
):
    await get_default_semantic_min_score("kbid1")
    await get_default_semantic_min_score("kbid1")
    await get_default_semantic_min_score("kbid1")

    await get_default_semantic_min_score("kbid2")

    assert get_kb_model_default_min_score_mock.call_count == 2

    get_default_semantic_min_score.cache_clear()


@pytest.fixture()
def read_only_txn():
    txn = unittest.mock.AsyncMock()
    with unittest.mock.patch(
        f"{QUERY_MODULE}.get_read_only_transaction", return_value=txn
    ):
        yield txn


@pytest.fixture()
def driver(read_only_txn):
    driver = unittest.mock.MagicMock()
    driver.transaction.return_value.__aenter__.return_value = read_only_txn
    with unittest.mock.patch(f"{QUERY_MODULE}.get_driver", return_value=driver):
        yield driver


@pytest.fixture()
def kbdm(read_only_txn, driver):
    kbdm = unittest.mock.AsyncMock()
    with unittest.mock.patch(
        f"{QUERY_MODULE}.KnowledgeBoxDataManager", return_value=kbdm
    ):
        yield kbdm


async def test_get_kb_model_default_min_score(kbdm):
    # If min_score is set, it should return it
    kbdm.get_model_metadata.return_value = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE,
        default_min_score=1.5,
    )
    assert await get_kb_model_default_min_score("kbid") == 1.5


async def test_get_kb_model_default_min_score_backward_compatible(kbdm):
    # If min_score is not set yet, it should return None
    kbdm.get_model_metadata.return_value = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE
    )
    assert await get_kb_model_default_min_score("kbid") is None


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
            filters=[],
            faceted=[],
            page_number=0,
            page_size=10,
            min_score=MinScore(vector=0.5),
            with_synonyms=True,
        )
        with patch("nucliadb.search.search.query.get_kb_synonyms", get_synonyms):
            yield qp

    @pytest.mark.asyncio
    async def test_not_applies_if_empty_body(
        self, query_parser: QueryParser, get_synonyms
    ):
        query_parser.query = ""
        search_request = Mock()
        await query_parser.parse_synonyms(search_request)

        get_synonyms.assert_not_awaited()
        search_request.ClearField.assert_not_called()

    @pytest.mark.asyncio
    async def test_not_applies_if_synonyms_object_not_found(
        self, query_parser: QueryParser, get_synonyms
    ):
        query_parser.query = "planet"
        get_synonyms.return_value = None
        request = Mock()

        await query_parser.parse_synonyms(Mock())

        request.ClearField.assert_not_called()
        get_synonyms.assert_awaited_once_with("kbid")

    @pytest.mark.asyncio
    async def test_not_applies_if_synonyms_not_found_for_query(
        self, query_parser: QueryParser, get_synonyms
    ):
        query_parser.query = "foobar"
        request = Mock()

        await query_parser.parse_synonyms(request)

        request.ClearField.assert_not_called()

        query_parser.query = "planet"
        await query_parser.parse_synonyms(request)

        request.ClearField.assert_called_once_with("body")
        assert request.advanced_query == "planet OR earth OR globe"


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
