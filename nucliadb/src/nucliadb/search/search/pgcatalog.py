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

import logging
from collections import defaultdict
from typing import Any, Literal, Union, cast

from psycopg.rows import dict_row

from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search.search.query_parser.models import CatalogExpression, CatalogQuery
from nucliadb_models.labels import translate_system_to_alias_label
from nucliadb_models.search import (
    ResourceResult,
    Resources,
    SortField,
    SortOrder,
)
from nucliadb_telemetry import metrics

from .filters import translate_label

observer = metrics.Observer("pg_catalog_search", labels={"op": ""})
logger = logging.getLogger(__name__)


def _filter_operands(operands: list[CatalogExpression]) -> tuple[list[str], list[CatalogExpression]]:
    facets = []
    nonfacets = []
    for op in operands:
        if op.facet:
            facets.append(op.facet)
        else:
            nonfacets.append(op)

    return facets, nonfacets


def _convert_filter(expr: CatalogExpression, filter_params: dict[str, Any]) -> str:
    if expr.bool_and:
        return _convert_boolean_op(expr.bool_and, "and", filter_params)
    elif expr.bool_or:
        return _convert_boolean_op(expr.bool_or, "or", filter_params)
    elif expr.bool_not:
        return f"(NOT {_convert_filter(expr.bool_not, filter_params)})"
    elif expr.date:
        return _convert_date_filter(expr.date, filter_params)
    elif expr.facet:
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = [expr.facet]
        return f"extract_facets(labels) @> %({param_name})s"
    elif expr.resource_id:
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = [expr.resource_id]
        return f"rid = %({param_name})s"
    else:
        return ""


def _convert_boolean_op(
    operands: list[CatalogExpression],
    op: Union[Literal["and"], Literal["or"]],
    filter_params: dict[str, Any],
) -> str:
    array_op = "@>" if op == "and" else "&&"
    sql = []
    facets, nonfacets = _filter_operands(operands)
    if facets:
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = facets
        sql.append(f"extract_facets(labels) {array_op} %({param_name})s")
    for nonfacet in nonfacets:
        sql.append(_convert_filter(nonfacet, filter_params))
    return "(" + f" {op.upper()} ".join(sql) + ")"


def _convert_date_filter(date: CatalogExpression.Date, filter_params: dict[str, Any]) -> str:
    if date.since and date.until:
        since_name = f"param{len(filter_params)}"
        filter_params[since_name] = date.since
        until_name = f"param{len(filter_params)}"
        filter_params[until_name] = date.until
        return f"{date.field} BETWEEN %({since_name})s AND %({until_name})s"
    elif date.since:
        since_name = f"param{len(filter_params)}"
        filter_params[since_name] = date.since
        return f"{date.field} > %({since_name})s"
    elif date.until:
        until_name = f"param{len(filter_params)}"
        filter_params[until_name] = date.until
        return f"{date.field} < %({until_name})s"
    else:
        raise ValueError(f"Invalid date operator")


def _prepare_query_filters(catalog_query: CatalogQuery) -> tuple[str, dict[str, Any]]:
    filter_sql = ["kbid = %(kbid)s"]
    filter_params: dict[str, Any] = {"kbid": catalog_query.kbid}

    if catalog_query.query:
        # This is doing tokenization inside the SQL server (to keep the index updated). We could move it to
        # the python code at update/query time if it ever becomes a problem but for now, a single regex
        # executed per query is not a problem.
        filter_sql.append(
            "regexp_split_to_array(lower(title), '\\W') @> regexp_split_to_array(lower(%(query)s), '\\W')"
        )
        filter_params["query"] = catalog_query.query

    if catalog_query.filters:
        filter_sql.append(_convert_filter(catalog_query.filters, filter_params))

    return (
        f"SELECT * FROM catalog WHERE {' AND '.join(filter_sql)}",
        filter_params,
    )


def _prepare_query(catalog_query: CatalogQuery) -> tuple[str, dict[str, Any]]:
    # Base query with all the filters
    query, filter_params = _prepare_query_filters(catalog_query)

    # Sort
    if catalog_query.sort:
        if catalog_query.sort.field == SortField.CREATED:
            order_field = "created_at"
        elif catalog_query.sort.field == SortField.MODIFIED:
            order_field = "modified_at"
        elif catalog_query.sort.field == SortField.TITLE:
            order_field = "title"
        else:
            # Deprecated order by score, use created_at instead
            order_field = "created_at"

        if catalog_query.sort.order == SortOrder.ASC:
            order_dir = "ASC"
        else:
            order_dir = "DESC"

        query += f" ORDER BY {order_field} {order_dir}"

    # Pagination
    offset = catalog_query.page_size * catalog_query.page_number
    query += f" LIMIT %(page_size)s OFFSET %(offset)s"
    filter_params["page_size"] = catalog_query.page_size
    filter_params["offset"] = offset

    return query, filter_params


def _pg_driver() -> PGDriver:
    return cast(PGDriver, get_driver())


@observer.wrap({"op": "search"})
async def pgcatalog_search(catalog_query: CatalogQuery) -> Resources:
    # Prepare SQL query
    query, query_params = _prepare_query_filters(catalog_query)

    async with _pg_driver()._get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        facets = {}

        # Faceted search
        if catalog_query.faceted:
            with observer({"op": "facets"}):
                tmp_facets: dict[str, dict[str, int]] = {
                    translate_label(f): defaultdict(int) for f in catalog_query.faceted
                }
                facet_filters = " OR ".join(f"label LIKE '{f}/%%'" for f in tmp_facets.keys())
                for facet in tmp_facets.keys():
                    if not (
                        facet.startswith("/n/s") or facet.startswith("/n/i") or facet.startswith("/l")
                    ):
                        logger.warn(
                            f"Unexpected facet used at catalog: {facet}, kbid={catalog_query.kbid}"
                        )

                await cur.execute(
                    f"SELECT label, COUNT(*) FROM (SELECT unnest(labels) AS label FROM ({query}) fc) nl WHERE ({facet_filters}) GROUP BY 1 ORDER BY 1",
                    query_params,
                )

                for row in await cur.fetchall():
                    label = row["label"]
                    label_parts = label.split("/")
                    parent = "/".join(label_parts[:-1])
                    count = row["count"]
                    if parent in tmp_facets:
                        tmp_facets[parent][translate_system_to_alias_label(label)] = count

                    # No need to get recursive because our facets are at most 3 levels deep (e.g: /l/set/label)
                    if len(label_parts) >= 3:
                        grandparent = "/".join(label_parts[:-2])
                        if grandparent in tmp_facets:
                            tmp_facets[grandparent][translate_system_to_alias_label(parent)] += count

                facets = {translate_system_to_alias_label(k): v for k, v in tmp_facets.items()}

        # Totals
        with observer({"op": "totals"}):
            await cur.execute(
                f"SELECT COUNT(*) FROM ({query}) fc",
                query_params,
            )
            total = (await cur.fetchone())["count"]  # type: ignore

        # Query
        with observer({"op": "query"}):
            query, query_params = _prepare_query(catalog_query)
            await cur.execute(query, query_params)
            data = await cur.fetchall()

    return Resources(
        facets=facets,
        results=[
            ResourceResult(
                rid=str(r["rid"]).replace("-", ""),
                field="title",
                field_type="a",
                labels=[label for label in r["labels"] if label.startswith("/l/")],
                score=0,
            )
            for r in data
        ],
        query=catalog_query.query,
        total=total,
        page_number=catalog_query.page_number,
        page_size=catalog_query.page_size,
        next_page=(catalog_query.page_size * catalog_query.page_number + len(data) < total),
        min_score=0,
    )
