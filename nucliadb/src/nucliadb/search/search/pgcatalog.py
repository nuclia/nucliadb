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

from typing import Any, Optional

from psycopg.rows import dict_row

from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb_models.search import SortField, SortOrder
from nucliadb_protos.nodereader_pb2 import (
    DocumentResult,
    DocumentSearchResponse,
    FacetResult,
    FacetResults,
    SearchResponse,
)

from .filters import translate_label
from .query import QueryParser


def _filter_operands(operands):
    literals = []
    nonliterals = []
    for operand in operands:
        op, params = next(iter(operand.items()))
        if op == "literal":
            literals.append(params)
        else:
            nonliterals.append(operand)

    return literals, nonliterals


def _convert_filter(filter, filter_params):
    op, operands = next(iter(filter.items()))
    if op == "literal":
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = [operands]
        return f"labels @> %({param_name})s"
    elif op in ("and", "or"):
        array_op = "@>" if op == "and" else "&&"
        sql = []
        literals, nonliterals = _filter_operands(operands)
        if literals:
            param_name = f"param{len(filter_params)}"
            filter_params[param_name] = literals
            sql.append(f"labels {array_op} %({param_name})s")
        for nonlit in nonliterals:
            sql.append(_convert_filter(nonlit, filter_params))
        return f" {op.upper()} ".join([f"({s})" for s in sql])
    elif op == "not":
        return f"NOT ({_convert_filter(operands[0], filter_params)})"
    else:
        raise ValueError(f"Invalid operator {op}")


def _prepare_query(query_parser: QueryParser):
    filter_sql = ["kbid = %(kbid)s"]
    filter_params: dict[str, Any] = {"kbid": query_parser.kbid}

    if query_parser.query:
        # This is doing tokenization inside the SQL server (to keep the index updated). We could move it to
        # the python code at update/query time if it ever becomes a problem but for now, a single regex
        # executed per query is not a problem.
        filter_sql.append(
            "regexp_split_to_array(lower(title), '\\W') @> regexp_split_to_array(lower(%(query)s), '\\W')"
        )
        filter_params["query"] = query_parser.query

    if query_parser.range_creation_start:
        filter_sql.append("created_at > %(created_at_start)s")
        filter_params["created_at_start"] = query_parser.range_creation_start

    if query_parser.range_creation_end:
        filter_sql.append("created_at < %(created_at_end)s")
        filter_params["created_at_end"] = query_parser.range_creation_end

    if query_parser.range_modification_start:
        filter_sql.append("modified_at > %(modified_at_start)s")
        filter_params["modified_at_start"] = query_parser.range_modification_start

    if query_parser.range_modification_end:
        filter_sql.append("modified_at < %(modified_at_end)s")
        filter_params["modified_at_end"] = query_parser.range_modification_end

    if query_parser.filters:
        filter_sql.append(_convert_filter(query_parser.filters, filter_params))

    if query_parser.sort:
        if query_parser.sort.field == SortField.CREATED:
            order_field = "created_at"
        elif query_parser.sort.field == SortField.MODIFIED:
            order_field = "modified_at"
        elif query_parser.sort.field == SortField.TITLE:
            order_field = "title"
        else:
            raise ValueError("Unsupported sort")

        if query_parser.sort.order == SortOrder.ASC:
            order_dir = "ASC"
        else:
            order_dir = "DESC"

    return (
        f"SELECT * FROM catalog WHERE {' AND '.join(filter_sql)} ORDER BY {order_field} {order_dir}",
        filter_params,
    )


def _pg_driver() -> Optional[PGDriver]:
    driver = get_driver()
    if isinstance(driver, PGDriver):
        return driver
    else:
        return None


def pgcatalog_enabled(kbid):
    return _pg_driver() is not None  # and has_feature(
    #     const.Features.PG_CATALOG_READ, context={"kbid": kbid}
    # )


async def pgcatalog_search(query_parser: QueryParser):
    driver = _pg_driver()
    if not driver:
        raise Exception("Cannot use PG catalog with non-PG driver")

    # Prepare SQL query
    query, query_params = _prepare_query(query_parser)

    async with driver._get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        facets: dict[str, FacetResults] = {}

        # Faceted search
        if query_parser.faceted:
            tmp_facets: dict[str, list[FacetResult]] = {
                translate_label(f): [] for f in query_parser.faceted
            }
            await cur.execute(
                f"SELECT unnest(labels) AS label, COUNT(*) FROM ({query}) GROUP BY 1 ORDER BY 1",
                query_params,
            )
            for row in await cur.fetchall():
                label = row["label"]
                parent = "/".join(label.split("/")[:-1])
                count = row["count"]
                if parent in tmp_facets:
                    tmp_facets[parent].append(FacetResult(tag=label, total=count))

            facets = {k: FacetResults(facetresults=v) for k, v in tmp_facets.items()}

        # Totals
        await cur.execute(
            f"SELECT COUNT(*) FROM ({query})",
            query_params,
        )
        total = (await cur.fetchone())["count"]  # type: ignore

        # Query
        await cur.execute(
            f"{query} LIMIT %(page_size)s OFFSET %(offset)s",
            {
                **query_params,
                "page_size": query_parser.page_size,
                "offset": query_parser.page_size * query_parser.page_number,
            },
        )
        data = await cur.fetchall()

    return SearchResponse(
        document=DocumentSearchResponse(
            results=[
                DocumentResult(uuid=str(r["rid"]).replace("-", ""), field="/a/title") for r in data
            ],
            facets=facets,
            total=total,
            page_number=query_parser.page_number,
        )
    )