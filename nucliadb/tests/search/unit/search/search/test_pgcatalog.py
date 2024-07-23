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

from nucliadb.search.search.pgcatalog import QueryParser, _convert_filter, _prepare_query
from nucliadb_models.search import (
    SortField,
    SortOptions,
    SortOrder,
)


def test_simple_filter():
    filter_params = {}
    query = _convert_filter({"literal": "/l/vegetable/potato"}, filter_params)
    assert query == "labels @> %(param0)s"
    assert filter_params == {"param0": ["/l/vegetable/potato"]}


def test_any_filter():
    filter_params = {}
    query = _convert_filter(
        {"or": [{"literal": "/l/vegetable/potato"}, {"literal": "/l/vegetable/carrot"}]}, filter_params
    )
    assert query == "(labels && %(param0)s)"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


def test_all_filter():
    filter_params = {}
    query = _convert_filter(
        {"and": [{"literal": "/l/vegetable/potato"}, {"literal": "/l/vegetable/carrot"}]}, filter_params
    )
    assert query == "(labels @> %(param0)s)"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


def test_none_filter():
    filter_params = {}
    query = _convert_filter(
        {"not": {"or": [{"literal": "/l/vegetable/potato"}, {"literal": "/l/vegetable/carrot"}]}},
        filter_params,
    )
    assert query == "(NOT (labels && %(param0)s))"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


def test_not_all_filter():
    filter_params = {}
    query = _convert_filter(
        {"not": {"and": [{"literal": "/l/vegetable/potato"}, {"literal": "/l/vegetable/carrot"}]}},
        filter_params,
    )
    assert query == "(NOT (labels @> %(param0)s))"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


def test_catalog_filter():
    filter_params = {}
    query = _convert_filter(
        {
            "and": [
                {"or": [{"literal": "/l/vegetable/potato"}, {"literal": "/l/vegetable/carrot"}]},
                {"or": [{"literal": "/n/s/PENDING"}, {"literal": "/n/s/PROCESSED"}]},
            ]
        },
        filter_params,
    )
    assert query == "((labels && %(param0)s) AND (labels && %(param1)s))"
    assert filter_params == {
        "param0": ["/l/vegetable/potato", "/l/vegetable/carrot"],
        "param1": ["/n/s/PENDING", "/n/s/PROCESSED"],
    }


def test_prepare_query_sort():
    parser = QueryParser(
        kbid="84ed9257-04ef-41d1-b1d2-26286b92777f",
        features=[],  # Ignored by pgcatalog
        query="",
        filters=[],
        page_number=0,
        page_size=25,
        sort=SortOptions(field=SortField.CREATED, order=SortOrder.ASC),
        min_score=0,  # Ignored by pgcatalog
    )
    query, params = _prepare_query(parser)
    assert "ORDER BY created_at ASC" in query

    parser = QueryParser(
        kbid="84ed9257-04ef-41d1-b1d2-26286b92777f",
        features=[],  # Ignored by pgcatalog
        query="",
        filters=[],
        page_number=0,
        page_size=25,
        sort=SortOptions(field=SortField.MODIFIED, order=SortOrder.DESC),
        min_score=0,  # Ignored by pgcatalog
    )
    query, params = _prepare_query(parser)
    assert "ORDER BY modified_at DESC" in query


def test_prepare_query_filters_kbid():
    parser = QueryParser(
        kbid="84ed9257-04ef-41d1-b1d2-26286b92777f",
        features=[],  # Ignored by pgcatalog
        query="",
        filters=[],
        page_number=0,
        page_size=25,
        min_score=0,  # Ignored by pgcatalog
    )
    query, params = _prepare_query(parser)
    assert "kbid = %(kbid)s" in query
    assert params["kbid"] == parser.kbid


def test_prepare_query_fulltext():
    parser = QueryParser(
        kbid="84ed9257-04ef-41d1-b1d2-26286b92777f",
        features=[],  # Ignored by pgcatalog
        query="This is my query",
        filters=[],
        page_number=0,
        page_size=25,
        min_score=0,  # Ignored by pgcatalog
    )
    query, params = _prepare_query(parser)
    assert (
        "regexp_split_to_array(lower(title), '\\W') @> regexp_split_to_array(lower(%(query)s), '\\W')"
        in query
    )
    assert params["query"] == parser.query
