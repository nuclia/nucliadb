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

import pytest
from nucliadb_protos.knowledgebox_pb2 import SemanticModelMetadata
from nucliadb_protos.nodereader_pb2 import SearchRequest
from nucliadb_protos.utils_pb2 import RelationNode, VectorSimilarity

from nucliadb.search.search.query import (
    get_default_min_score,
    get_kb_model_default_min_score,
    parse_entities_to_filters,
)

QUERY_MODULE = "nucliadb.search.search.query"


def test_parse_entities_to_filters():
    detected_entities = [
        RelationNode(value="John", ntype=RelationNode.NodeType.ENTITY, subtype="person")
    ]

    request = SearchRequest()
    assert parse_entities_to_filters(request, detected_entities) == ["/e/person/John"]
    assert request.filter.tags == ["/e/person/John"]

    assert parse_entities_to_filters(request, detected_entities) == []
    assert request.filter.tags == ["/e/person/John"]


@pytest.fixture()
def get_kb_model_default_min_score_mock():
    with unittest.mock.patch(f"{QUERY_MODULE}.get_kb_model_default_min_score") as mock:
        yield mock


@pytest.fixture(scope="function")
def has_feature():
    with unittest.mock.patch(f"{QUERY_MODULE}.has_feature", return_value=True) as mock:
        yield mock


async def test_get_default_min_score(has_feature, get_kb_model_default_min_score_mock):
    get_kb_model_default_min_score_mock.return_value = 1.5

    assert await get_default_min_score("kbid") == 1.5

    get_default_min_score.cache_clear()


async def test_get_default_min_score_default_value(
    has_feature, get_kb_model_default_min_score_mock
):
    get_kb_model_default_min_score_mock.return_value = None

    assert await get_default_min_score("kbid") == 0.7

    get_default_min_score.cache_clear()


async def test_get_default_min_score_is_cached(
    has_feature, get_kb_model_default_min_score_mock
):
    await get_default_min_score("kbid1")
    await get_default_min_score("kbid1")
    await get_default_min_score("kbid1")

    await get_default_min_score("kbid2")

    assert get_kb_model_default_min_score_mock.call_count == 2

    get_default_min_score.cache_clear()


async def test_get_default_min_score_feature_not_enabled(
    has_feature, get_kb_model_default_min_score_mock
):
    has_feature.return_value = False
    get_kb_model_default_min_score_mock.return_value = 1.5

    assert await get_default_min_score("kbid1") == 0.7


@pytest.fixture()
def txn():
    txn = unittest.mock.AsyncMock()
    yield txn


@pytest.fixture()
def driver(txn):
    driver = unittest.mock.MagicMock()
    driver.transaction.return_value.__aenter__.return_value = txn
    with unittest.mock.patch(f"{QUERY_MODULE}.get_driver", return_value=driver):
        yield driver


@pytest.fixture()
def storage():
    storage = unittest.mock.AsyncMock()
    with unittest.mock.patch(f"{QUERY_MODULE}.get_storage", return_value=storage):
        yield storage


@pytest.fixture()
def kb_get_model_metadata(driver, storage):
    with unittest.mock.patch(f"{QUERY_MODULE}.KnowledgeBox.get_model_metadata") as mock:
        yield mock


async def test_get_kb_model_default_min_score(has_feature, kb_get_model_metadata):
    # If min_score is set, it should return it
    kb_get_model_metadata.return_value = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE,
        default_min_score=1.5,
    )
    assert await get_kb_model_default_min_score("kbid") == 1.5


async def test_get_kb_model_default_min_score_backward_compatible(
    has_feature,
    kb_get_model_metadata,
):
    # If min_score is not set yet, it should return None
    kb_get_model_metadata.return_value = SemanticModelMetadata(
        similarity_function=VectorSimilarity.COSINE
    )
    assert await get_kb_model_default_min_score("kbid") is None
