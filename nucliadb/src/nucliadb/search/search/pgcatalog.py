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

from psycopg.rows import dict_row

from nucliadb.common.maindb.utils import get_driver
from nucliadb_models.search import SortField, SortOrder
from nucliadb_protos.nodereader_pb2 import (
    DocumentResult,
    DocumentSearchResponse,
    FacetResult,
    FacetResults,
    SearchResponse,
)

from .query import QueryParser


async def pgcatalog_search(query_parser: QueryParser):
    async with get_driver()._get_connection() as conn, conn.cursor(row_factory=dict_row) as cur:
        #
        # Faceted search
        #
        facets = {}

        facet_status = False
        facet_labels = []
        facet_icon = []

        for f in query_parser.faceted:
            if f == "/metadata.status":
                facet_status = True
            elif f.startswith("/icon"):
                facet_icon.append(f)
            elif f.startswith("/l"):
                facet_labels.append(f)
            else:
                raise "Unknown facet"

        if facet_status:
            await cur.execute(
                "SELECT status, COUNT(*) FROM catalog WHERE kbid = %(kbid)s GROUP BY status",
                {"kbid": query_parser.kbid},
            )
            facets["/n/s"] = FacetResults(
                facetresults=[
                    FacetResult(tag="/n/s/" + row["status"].upper(), total=row["count"])
                    for row in await cur.fetchall()
                ]
            )

        if facet_icon:
            await cur.execute(
                "SELECT SPLIT_PART(mimetype, '/',1) AS facet, mimetype AS tag, COUNT(*) AS total FROM catalog WHERE kbid = %(kbid)s AND mimetype IS NOT NULL GROUP BY 1, 2 ORDER BY 1, 2",
                {"kbid": query_parser.kbid},
            )
            from collections import defaultdict

            grouped = defaultdict(list)
            for row in await cur.fetchall():
                grouped["/icon/" + row["facet"]].append(row)

            for key, row in grouped.items():
                if key not in facet_icon:
                    print(f"DIDN'T ASK FOR mime {key}")
                    continue
                facets[key] = FacetResults(
                    facetresults=[FacetResult(tag="/n/i/" + r["tag"], total=r["total"]) for r in row]
                )

        if facet_labels:
            await cur.execute(
                "SELECT SPLIT_PART(UNNEST(labels), '/', 3) AS facet, UNNEST(labels) AS tag, COUNT(*) AS total FROM catalog WHERE kbid = %(kbid)s GROUP BY 1, 2 ORDER BY 1, 2",
                {"kbid": query_parser.kbid},
            )
            from collections import defaultdict

            grouped = defaultdict(list)
            for row in await cur.fetchall():
                grouped["/l/" + row["facet"]].append(row)

            for key, row in grouped.items():
                if key not in facet_labels:
                    print(f"DIDN'T ASK FOR label {key}")
                    continue
                facets[key] = FacetResults(
                    facetresults=[FacetResult(tag=r["tag"], total=r["total"]) for r in row]
                )

        #
        # Normal search
        #

        # Build filters
        filter_sql = ["kbid = %(kbid)s"]
        filter_params = {"kbid": query_parser.kbid}

        if query_parser.query:
            # TODO: Tokenizer?
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

        # TODO: Support arbitraty filters or at least what `convert_filter_to_node_schema` can generate
        filter_labels = []
        filter_types = []
        filter_statuses = []
        for op, operands in query_parser.filters.items():
            if op == "literal":
                op = "and"
                operands = [{"literal": operands}]
            elif op == "or":
                op = "and"
                operands = [{"or": operands}]
            if op != "and":
                raise "Unsupported filter (no and)"
            for o in operands:
                for k, v in o.items():
                    if k == "literal":
                        k = "or"
                        v = [{"literal": v}]
                    if k != "or":
                        raise "Unsupported filter (no or)"

                    for i in v:
                        for literal, value in i.items():
                            if literal != "literal":
                                raise "Unsupported filter (no literal)"
                            value = value.replace("/classification.labels", "/l").replace(
                                "/metadata.status", "/n/s"
                            )

                            if value.startswith("/n/i"):
                                filter_types.append(value)
                            elif value.startswith("/n/s"):
                                filter_statuses.append(value)
                            elif value.startswith("/l"):
                                filter_labels.append(value)
                            else:
                                raise Exception(f"Unsupported filter {value}")

        if filter_labels:
            filter_sql.append("labels && %(labels)s")
            filter_params["labels"] = filter_labels

        if filter_types:
            filter_sql.append("labels && %(mimetypes)s")
            filter_params["mimetypes"] = filter_types

        if filter_statuses:
            filter_sql.append("labels && %(statuses)s")
            filter_params["statuses"] = filter_statuses

        print(query_parser.filters)
        print(filter_sql, filter_params)

        def pg_to_pb(rows, facets, total):
            return SearchResponse(
                document=DocumentSearchResponse(
                    results=[
                        DocumentResult(uuid=str(r["rid"]).replace("-", ""), field="/a/title")
                        for r in rows
                    ],
                    facets=facets,
                    total=total,
                    page_number=query_parser.page_number,
                )
            )

        if query_parser.sort.field == SortField.CREATED:
            order_field = "created_at"
        elif query_parser.sort.field == SortField.MODIFIED:
            order_field = "modified_at"
        elif query_parser.sort.field == SortField.TITLE:
            order_field = "title"
        else:
            raise "Unsupported sort"

        if query_parser.sort.order == SortOrder.ASC:
            order_dir = "ASC"
        else:
            order_dir = "DESC"

        await cur.execute(
            f"SELECT COUNT(*) FROM catalog WHERE {' AND '.join(filter_sql)}",
            filter_params,
        )
        total = (await cur.fetchone())["count"]

        await cur.execute(
            f"SELECT * FROM catalog WHERE {' AND '.join(filter_sql)} ORDER BY {order_field} {order_dir} LIMIT %(page_size)s OFFSET %(offset)s",
            {
                **filter_params,
                "page_size": query_parser.page_size,
                "offset": query_parser.page_size * query_parser.page_number,
            },
        )
        data = await cur.fetchall()
        result = pg_to_pb(data, facets, total)

        return result
