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
import re
from collections import defaultdict
from typing import Any, Literal, Union, cast

from psycopg import AsyncCursor, sql
from psycopg.rows import DictRow, dict_row

from nucliadb.common.maindb.pg import PGDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.search.search.query_parser.models import CatalogExpression, CatalogQuery
from nucliadb_models import search as search_models
from nucliadb_models.labels import translate_system_to_alias_label
from nucliadb_models.search import CatalogFacetsRequest, ResourceResult, Resources, SortField, SortOrder
from nucliadb_telemetry import metrics

from .filters import translate_label

observer = metrics.Observer("pg_catalog_search", labels={"op": ""})
logger = logging.getLogger(__name__)

SPLIT_REGEX = re.compile(r"\W")


def _filter_operands(operands: list[CatalogExpression]) -> tuple[list[str], list[CatalogExpression]]:
    facets = []
    nonfacets = []
    for op in operands:
        if op.facet:
            facets.append(op.facet)
        else:
            nonfacets.append(op)

    return facets, nonfacets


def _convert_filter(expr: CatalogExpression, filter_params: dict[str, Any]) -> sql.Composable:
    if expr.bool_and:
        return _convert_boolean_op(expr.bool_and, "and", filter_params)
    elif expr.bool_or:
        return _convert_boolean_op(expr.bool_or, "or", filter_params)
    elif expr.bool_not:
        return sql.SQL("(NOT {})").format(_convert_filter(expr.bool_not, filter_params))
    elif expr.date:
        return _convert_date_filter(expr.date, filter_params)
    elif expr.facet:
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = [expr.facet]
        if expr.facet == "/n/s/PROCESSED":
            # Optimization for the most common case, we know PROCESSED is a full label and can use the smaller labels index
            # This is needed because PROCESSED is present in most catalog entries and PG is unlikely to use any index
            # for it, falling back to executing the extract_facets function which can be slow
            return sql.SQL("labels @> {}").format(sql.Placeholder(param_name))
        else:
            return sql.SQL("extract_facets(labels) @> {}").format(sql.Placeholder(param_name))
    elif expr.resource_id:
        param_name = f"param{len(filter_params)}"
        filter_params[param_name] = [expr.resource_id]
        return sql.SQL("rid = {}").format(sql.Placeholder(param_name))
    else:
        return sql.SQL("")


def _convert_boolean_op(
    operands: list[CatalogExpression],
    op: Union[Literal["and"], Literal["or"]],
    filter_params: dict[str, Any],
) -> sql.Composable:
    array_op = sql.SQL("@>" if op == "and" else "&&")
    operands_sql: list[sql.Composable] = []
    facets, nonfacets = _filter_operands(operands)
    if facets:
        param_name = f"param{len(filter_params)}"
        if facets == ["/n/s/PROCESSED"]:
            # Optimization for the most common case, we know PROCESSED is a full label and can use the smaller labels index
            # This is needed because PROCESSED is present in most catalog entries and PG is unlikely to use any index
            # for it, falling back to executing the extract_facets function which can be slow
            operands_sql.append(sql.SQL("labels @> {}").format(sql.Placeholder(param_name)))
        else:
            operands_sql.append(
                sql.SQL("extract_facets(labels) {} {}").format(array_op, sql.Placeholder(param_name))
            )
        filter_params[param_name] = facets
    for nonfacet in nonfacets:
        operands_sql.append(_convert_filter(nonfacet, filter_params))
    return sql.SQL("({})").format(sql.SQL(f" {op.upper()} ").join(operands_sql))


def _convert_date_filter(date: CatalogExpression.Date, filter_params: dict[str, Any]) -> sql.Composable:
    if date.since and date.until:
        since_name = f"param{len(filter_params)}"
        filter_params[since_name] = date.since
        until_name = f"param{len(filter_params)}"
        filter_params[until_name] = date.until
        return sql.SQL("{field} BETWEEN {since} AND {until}").format(
            field=sql.Identifier(date.field),
            since=sql.Placeholder(since_name),
            until=sql.Placeholder(until_name),
        )
    elif date.since:
        since_name = f"param{len(filter_params)}"
        filter_params[since_name] = date.since
        return sql.SQL("{field} > {since}").format(
            field=sql.Identifier(date.field), since=sql.Placeholder(since_name)
        )
    elif date.until:
        until_name = f"param{len(filter_params)}"
        filter_params[until_name] = date.until
        return sql.SQL("{field} < {until}").format(
            field=sql.Identifier(date.field), until=sql.Placeholder(until_name)
        )
    else:
        raise ValueError(f"Invalid date operator")


def _prepare_query_filters(catalog_query: CatalogQuery) -> tuple[sql.Composable, dict[str, Any]]:
    filter_sql: list[sql.Composable] = [sql.SQL("kbid = %(kbid)s")]
    filter_params: dict[str, Any] = {"kbid": catalog_query.kbid}

    if catalog_query.query and catalog_query.query.query:
        filter_sql.append(_prepare_query_search(catalog_query.query, filter_params))

    if catalog_query.filters:
        filter_sql.append(_convert_filter(catalog_query.filters, filter_params))

    return (
        sql.SQL("SELECT * FROM catalog WHERE {}").format(sql.SQL(" AND ").join(filter_sql)),
        filter_params,
    )


def _prepare_query_search(query: search_models.CatalogQuery, params: dict[str, Any]) -> sql.Composable:
    if query.match == search_models.CatalogQueryMatch.Exact:
        params["query"] = query.query
        return sql.SQL("{} = %(query)s").format(sql.Identifier(query.field.value))
    elif query.match == search_models.CatalogQueryMatch.StartsWith:
        params["query"] = query.query + "%"
        if query.field == search_models.CatalogQueryField.Title:
            # Insensitive search supported by pg_trgm for title
            return sql.SQL("{} ILIKE %(query)s").format(sql.Identifier(query.field.value))
        else:
            # Sensitive search for slug (btree does not support ILIKE and slugs are all lowercase anyway)
            return sql.SQL("{} LIKE %(query)s").format(sql.Identifier(query.field.value))
    # The rest of operators only supported by title
    elif query.match == search_models.CatalogQueryMatch.Words:
        # This is doing tokenization inside the SQL server (to keep the index updated). We could move it to
        # the python code at update/query time if it ever becomes a problem but for now, a single regex
        # executed per query is not a problem.

        # Remove zero-length words from the split
        params["query"] = [word.lower() for word in SPLIT_REGEX.split(query.query) if word]
        return sql.SQL("regexp_split_to_array(lower(title), '\\W') @> %(query)s")
    elif query.match == search_models.CatalogQueryMatch.Fuzzy:
        params["query"] = query.query
        # Note: the operator is %>, We use %%> for psycopg escaping
        return sql.SQL("title %%> %(query)s")
    elif query.match == search_models.CatalogQueryMatch.EndsWith:
        params["query"] = "%" + query.query
        return sql.SQL("title ILIKE %(query)s")
    elif query.match == search_models.CatalogQueryMatch.Contains:
        params["query"] = "%" + query.query + "%"
        return sql.SQL("title ILIKE %(query)s")
    else:  # pragma: nocover
        # This is a trick so mypy generates an error if this branch can be reached,
        # that is, if we are missing some ifs
        _a: int = "a"
        return sql.SQL("")


def _prepare_query(catalog_query: CatalogQuery) -> tuple[sql.Composed, dict[str, Any]]:
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

        query += sql.SQL(" ORDER BY {} {}").format(sql.Identifier(order_field), sql.SQL(order_dir))

    # Pagination
    offset = catalog_query.page_size * catalog_query.page_number
    query += sql.SQL(" LIMIT %(page_size)s OFFSET %(offset)s")
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

                if catalog_query.filters is None:
                    await _faceted_search_unfiltered(cur, catalog_query, tmp_facets)
                else:
                    await _faceted_search_filtered(cur, catalog_query, tmp_facets, query, query_params)

                facets = {translate_system_to_alias_label(k): v for k, v in tmp_facets.items()}

        # Totals
        with observer({"op": "totals"}):
            await cur.execute(
                sql.SQL("SELECT COUNT(*) FROM ({}) fc").format(query),
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
        query=catalog_query.query.query if catalog_query.query else "",
        total=total,
        page_number=catalog_query.page_number,
        page_size=catalog_query.page_size,
        next_page=(catalog_query.page_size * catalog_query.page_number + len(data) < total),
        min_score=0,
    )


async def _faceted_search_unfiltered(
    cur: AsyncCursor[DictRow], catalog_query: CatalogQuery, tmp_facets: dict[str, dict[str, int]]
):
    facet_params: dict[str, Any] = {}
    facet_sql: sql.Composable
    if len(tmp_facets) <= 5:
        # Asking for few facets, strictly filter to what we need in the query
        prefixes_sql = []
        for cnt, prefix in enumerate(tmp_facets.keys()):
            prefixes_sql.append(
                sql.SQL("(facet LIKE {} AND POSITION('/' IN RIGHT(facet, {})) = 0)").format(
                    sql.Placeholder(f"facet_{cnt}"), sql.Placeholder(f"facet_len_{cnt}")
                )
            )
            facet_params[f"facet_{cnt}"] = f"{prefix}/%"
            facet_params[f"facet_len_{cnt}"] = -(len(prefix) + 1)
        facet_sql = sql.SQL("AND {}").format(sql.SQL(" OR ").join(prefixes_sql))
    elif all((facet.startswith("/l") or facet.startswith("/n/i") for facet in tmp_facets.keys())):
        # Special case for the catalog query, which can have many facets asked for
        # Filter for the categories (icon and labels) in the query, filter the rest in the code below
        facet_sql = sql.SQL("AND (facet LIKE '/l/%%' OR facet like '/n/i/%%')")
    else:
        # Worst case: ask for all facets and filter here. This is faster than applying lots of filters
        facet_sql = sql.SQL("")

    await cur.execute(
        sql.SQL(
            "SELECT facet, COUNT(*) FROM catalog_facets WHERE kbid = %(kbid)s {} GROUP BY facet"
        ).format(facet_sql),
        {"kbid": catalog_query.kbid, **facet_params},
    )

    # Only keep the facets we asked for
    for row in await cur.fetchall():
        facet = row["facet"]
        facet_parts = facet.split("/")
        parent = "/".join(facet_parts[:-1])
        if parent in tmp_facets:
            tmp_facets[parent][translate_system_to_alias_label(facet)] = row["count"]


async def _faceted_search_filtered(
    cur: AsyncCursor[DictRow],
    catalog_query: CatalogQuery,
    tmp_facets: dict[str, dict[str, int]],
    query: sql.Composable,
    query_params: dict[str, Any],
):
    facet_params = {}
    facet_filters = []
    for cnt, facet in enumerate(tmp_facets.keys()):
        facet_filters.append(sql.SQL("label LIKE {}").format(sql.Placeholder(f"facet_{cnt}")))
        facet_params[f"facet_{cnt}"] = f"{facet}/%"

    for facet in tmp_facets.keys():
        if not (facet.startswith("/n/s") or facet.startswith("/n/i") or facet.startswith("/l")):
            logger.warning(f"Unexpected facet used at catalog: {facet}, kbid={catalog_query.kbid}")

    await cur.execute(
        sql.SQL(
            "SELECT label, COUNT(*) FROM (SELECT unnest(labels) AS label FROM ({query}) fc) nl WHERE ({facet_filters}) GROUP BY 1 ORDER BY 1"
        ).format(query=query, facet_filters=sql.SQL(" OR ").join(facet_filters)),
        {**query_params, **facet_params},
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


@observer.wrap({"op": "catalog_facets"})
async def pgcatalog_facets(kbid: str, request: CatalogFacetsRequest) -> dict[str, int]:
    async with _pg_driver()._get_connection() as conn, conn.cursor() as cur:
        prefix_filters: list[sql.Composable] = []
        prefix_params: dict[str, Any] = {}
        for cnt, prefix in enumerate(request.prefixes):
            prefix_sql = sql.SQL("facet LIKE {}").format(sql.Placeholder(f"prefix{cnt}"))
            prefix_params[f"prefix{cnt}"] = f"{prefix.prefix}%"
            if prefix.depth is not None:
                prefix_parts = len(prefix.prefix.split("/"))
                depth_sql = sql.SQL("SPLIT_PART(facet, '/', {}) = ''").format(
                    sql.Placeholder(f"depth{cnt}")
                )
                prefix_params[f"depth{cnt}"] = prefix_parts + prefix.depth + 1
                prefix_sql = sql.SQL("({} AND {})").format(prefix_sql, depth_sql)
            prefix_filters.append(prefix_sql)

        filter_sql: sql.Composable
        if prefix_filters:
            filter_sql = sql.SQL("AND {}").format(sql.SQL(" OR ").join(prefix_filters))
        else:
            filter_sql = sql.SQL("")

        await cur.execute(
            sql.SQL(
                "SELECT facet, COUNT(*) FROM catalog_facets WHERE kbid = %(kbid)s {} GROUP BY facet"
            ).format(filter_sql),
            {"kbid": kbid, **prefix_params},
        )
        return {k: v for k, v in await cur.fetchall()}
