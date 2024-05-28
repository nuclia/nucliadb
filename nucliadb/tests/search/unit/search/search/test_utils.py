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
import pytest

from nucliadb.search.search.utils import (
    has_user_vectors,
    is_empty_query,
    is_exact_match_only_query,
    should_disable_vector_search,
)
from nucliadb_models.search import SearchRequest


@pytest.mark.parametrize(
    "item,empty",
    [
        (SearchRequest(query=""), True),
        (SearchRequest(query="foo"), False),
    ],
)
def test_is_empty_query(item, empty):
    assert is_empty_query(item) is empty


@pytest.mark.parametrize(
    "query,exact_match",
    [
        ("some", False),
        ("some query terms", False),
        ('"something"', True),
        ('   "something"', True),
        ('"something"   ', True),
        ('"something exact"', True),
        ('"something exact" and something else', False),
    ],
)
def test_is_exact_match_only_query(query, exact_match):
    item = SearchRequest(query=query)
    assert is_exact_match_only_query(item) is exact_match


@pytest.mark.parametrize(
    "item,has_vectors",
    [
        (SearchRequest(query=""), False),
        (SearchRequest(vector=[]), False),
        (SearchRequest(vector=[1.0]), True),
    ],
)
def test_has_user_vectors(item, has_vectors):
    assert has_user_vectors(item) is has_vectors


@pytest.mark.parametrize(
    "item,disable_vectors",
    [
        (SearchRequest(query=""), True),
        (SearchRequest(query='"exact match"'), True),
        (SearchRequest(query="foo"), False),
        (SearchRequest(query="", vector=[1.0, 2.0]), False),
        (SearchRequest(query='"exact match"', vector=[1.0, 2.0]), False),
    ],
)
def test_should_disable_vectors(item, disable_vectors):
    assert should_disable_vector_search(item) is disable_vectors
