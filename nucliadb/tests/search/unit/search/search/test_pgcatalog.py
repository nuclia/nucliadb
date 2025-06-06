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
from datetime import datetime

from nucliadb.search.search.pgcatalog import _convert_filter, _prepare_query
from nucliadb.search.search.query_parser.models import CatalogExpression
from nucliadb.search.search.query_parser.parsers import parse_catalog
from nucliadb_models.filters import CatalogFilterExpression
from nucliadb_models.search import (
    CatalogRequest,
    SortField,
    SortOptions,
    SortOrder,
)


async def test_simple_filter():
    filter_params = {}
    query = _convert_filter(CatalogExpression(facet="/l/vegetable/potato"), filter_params)
    assert query.as_string() == "extract_facets(labels) @> %(param0)s"
    assert filter_params == {"param0": ["/l/vegetable/potato"]}


async def test_any_filter():
    filter_params = {}
    query = _convert_filter(
        CatalogExpression(
            bool_or=[
                CatalogExpression(facet="/l/vegetable/potato"),
                CatalogExpression(facet="/l/vegetable/carrot"),
            ]
        ),
        filter_params,
    )
    assert query.as_string() == "(extract_facets(labels) && %(param0)s)"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


async def test_all_filter():
    filter_params = {}
    query = _convert_filter(
        CatalogExpression(
            bool_and=[
                CatalogExpression(facet="/l/vegetable/potato"),
                CatalogExpression(facet="/l/vegetable/carrot"),
            ]
        ),
        filter_params,
    )
    assert query.as_string() == "(extract_facets(labels) @> %(param0)s)"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


async def test_none_filter():
    filter_params = {}
    query = _convert_filter(
        CatalogExpression(
            bool_not=CatalogExpression(
                bool_or=[
                    CatalogExpression(facet="/l/vegetable/potato"),
                    CatalogExpression(facet="/l/vegetable/carrot"),
                ]
            )
        ),
        filter_params,
    )
    assert query.as_string() == "(NOT (extract_facets(labels) && %(param0)s))"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


async def test_not_all_filter():
    filter_params = {}
    query = _convert_filter(
        CatalogExpression(
            bool_not=CatalogExpression(
                bool_and=[
                    CatalogExpression(facet="/l/vegetable/potato"),
                    CatalogExpression(facet="/l/vegetable/carrot"),
                ]
            )
        ),
        filter_params,
    )
    assert query.as_string() == "(NOT (extract_facets(labels) @> %(param0)s))"
    assert filter_params == {"param0": ["/l/vegetable/potato", "/l/vegetable/carrot"]}


async def test_catalog_filter():
    filter_params = {}
    query = _convert_filter(
        CatalogExpression(
            bool_and=[
                CatalogExpression(
                    bool_or=[
                        CatalogExpression(facet="/l/vegetable/potato"),
                        CatalogExpression(facet="/l/vegetable/carrot"),
                    ]
                ),
                CatalogExpression(
                    bool_or=[
                        CatalogExpression(facet="/n/s/PENDING"),
                        CatalogExpression(facet="/n/s/PROCESSED"),
                    ]
                ),
            ]
        ),
        filter_params,
    )
    assert (
        query.as_string()
        == "((extract_facets(labels) && %(param0)s) AND (extract_facets(labels) && %(param1)s))"
    )
    assert filter_params == {
        "param0": ["/l/vegetable/potato", "/l/vegetable/carrot"],
        "param1": ["/n/s/PENDING", "/n/s/PROCESSED"],
    }


async def test_prepare_query_sort():
    kbid = "84ed9257-04ef-41d1-b1d2-26286b92777f"
    request = CatalogRequest(
        query="",
        page_number=0,
        page_size=25,
        sort=SortOptions(field=SortField.CREATED, order=SortOrder.ASC),
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)
    assert 'ORDER BY "created_at" ASC' in query.as_string()

    request = CatalogRequest(
        query="",
        page_number=0,
        page_size=25,
        sort=SortOptions(field=SortField.MODIFIED, order=SortOrder.DESC),
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)
    assert 'ORDER BY "modified_at" DESC' in query.as_string()


async def test_prepare_query_filters_kbid():
    kbid = "84ed9257-04ef-41d1-b1d2-26286b92777f"
    request = CatalogRequest(
        query="",
        page_number=0,
        page_size=25,
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)
    assert "kbid = %(kbid)s" in query.as_string()
    assert params["kbid"] == parsed.kbid


async def test_prepare_query_fulltext():
    kbid = "84ed9257-04ef-41d1-b1d2-26286b92777f"
    request = CatalogRequest.model_validate(
        {
            "query": "This is my query",
            "page_number": 0,
            "page_size": 25,
        }
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)
    assert (
        "regexp_split_to_array(lower(title), '\\W') @> regexp_split_to_array(lower(%(query)s), '\\W')"
        in query.as_string()
    )
    assert params["query"] == "This is my query"


async def test_old_filters():
    kbid = "84ed9257-04ef-41d1-b1d2-26286b92777f"
    request = CatalogRequest.model_validate(
        {
            "query": "This is my query",
            "filters": ["/classification.labels/topic/boats"],
            "range_creation_start": datetime.fromisoformat("2020-01-01T00:00:00"),
            "page_number": 0,
            "page_size": 25,
        }
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)
    assert '"created_at" > ' in query.as_string()
    param_values = list(params.values())
    assert datetime.fromisoformat("2020-01-01T00:00:00") in param_values
    assert ["/l/topic/boats"] in param_values


async def test_filter_expression():
    kbid = "84ed9257-04ef-41d1-b1d2-26286b92777f"
    request = CatalogRequest.model_validate(
        {
            "query": "This is my query",
            "filter_expression": CatalogFilterExpression.model_validate(
                {
                    "resource": {
                        "or": [
                            {
                                "and": [
                                    {"prop": "label", "labelset": "topic", "label": "boats"},
                                    {"prop": "origin_path", "prefix": "folder"},
                                    {"not": {"prop": "modified", "since": "2019-01-01T11:00:00"}},
                                ]
                            },
                            {"prop": "resource", "id": "00112233445566778899aabbccddeeff"},
                        ]
                    }
                }
            ),
            "page_number": 0,
            "page_size": 25,
        }
    )
    parsed = await parse_catalog(kbid, request)
    query, params = _prepare_query(parsed)

    # This test is very sensitive to query generation changes
    assert query.as_string() == (
        "SELECT * FROM catalog "
        "WHERE kbid = %(kbid)s AND regexp_split_to_array(lower(title), '\\W') @> regexp_split_to_array(lower(%(query)s), '\\W') "
        "AND ("
        '(extract_facets(labels) @> %(param2)s AND (NOT "modified_at" > %(param3)s)) '
        "OR rid = %(param4)s"
        ") "
        'ORDER BY "created_at" DESC '
        "LIMIT %(page_size)s OFFSET %(offset)s"
    )
    assert params == {
        "kbid": "84ed9257-04ef-41d1-b1d2-26286b92777f",
        "param2": ["/l/topic/boats", "/p/folder"],
        "param3": datetime(2019, 1, 1, 11, 0),
        "param4": ["00112233445566778899aabbccddeeff"],
        "query": "This is my query",
        "page_size": 25,
        "offset": 0,
    }
