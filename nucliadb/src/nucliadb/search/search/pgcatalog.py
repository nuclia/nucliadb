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

from typing import Any, cast

from psycopg.rows import dict_row

from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb_models.labels import translate_system_to_alias_label
from nucliadb_models.metadata import ResourceProcessingStatus
from nucliadb_models.search import (
    ResourceResult,
    Resources,
    SortField,
    SortOrder,
)
from nucliadb_telemetry import metrics
from nucliadb_utils import const
from nucliadb_utils.utilities import has_feature

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
        return "(" + f" {op.upper()} ".join(sql) + ")"
    elif op == "not":
        return f"(NOT {_convert_filter(operands, filter_params)})"
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

    order_sql = ""
    if query_parser.sort:
        if query_parser.sort.field == SortField.CREATED:
            order_field = "created_at"
        elif query_parser.sort.field == SortField.MODIFIED:
            order_field = "modified_at"
        elif query_parser.sort.field == SortField.TITLE:
            order_field = "title"
        else:
            # Deprecated order by score, use created_at instead
            order_field = "created_at"

        if query_parser.sort.order == SortOrder.ASC:
            order_dir = "ASC"
        else:
            order_dir = "DESC"

        order_sql = f" ORDER BY {order_field} {order_dir}"

    if query_parser.with_status:
        filter_sql.append("labels && %(status)s")
        if query_parser.with_status == ResourceProcessingStatus.PROCESSED:
            filter_params["status"] = ["/n/s/PROCESSED", "/n/s/ERROR"]
        else:
            filter_params["status"] = ["/n/s/PENDING"]

    return (
        f"SELECT * FROM catalog WHERE {' AND '.join(filter_sql)}{order_sql}",
        filter_params,
    )


def _pg_driver() -> PGDriver:
    return cast(PGDriver, get_driver())


def pgcatalog_enabled(kbid):
    return isinstance(get_driver(), PGDriver) and has_feature(
        const.Features.PG_CATALOG_READ, context={"kbid": kbid}
    )


@metrics.Observer("pg_catalog_search").wrap()
async def pgcatalog_search(query_parser: QueryParser) -> Resources:
    # Prepare SQL query
    query, query_params = _prepare_query(query_parser)

    async with _pg_driver()._get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        facets = {}

        # Faceted search
        if query_parser.faceted:
            tmp_facets: dict[str, dict[str, int]] = {
                translate_label(f): {} for f in query_parser.faceted
            }
            await cur.execute(
                f"SELECT unnest(labels) AS label, COUNT(*) FROM ({query}) fc GROUP BY 1 ORDER BY 1",
                query_params,
            )
            for row in await cur.fetchall():
                label = row["label"]
                parent = "/".join(label.split("/")[:-1])
                count = row["count"]
                if parent in tmp_facets:
                    tmp_facets[parent][translate_system_to_alias_label(label)] = count

            facets = {translate_system_to_alias_label(k): v for k, v in tmp_facets.items()}

        # Totals
        await cur.execute(
            f"SELECT COUNT(*) FROM ({query}) fc",
            query_params,
        )
        total = (await cur.fetchone())["count"]  # type: ignore

        # Query
        offset = query_parser.page_size * query_parser.page_number
        await cur.execute(
            f"{query} LIMIT %(page_size)s OFFSET %(offset)s",
            {
                **query_params,
                "page_size": query_parser.page_size,
                "offset": offset,
            },
        )
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
        query=query_parser.query,
        total=total,
        page_number=query_parser.page_number,
        page_size=query_parser.page_size,
        next_page=(offset + len(data) < total),
        min_score=0,
    )
