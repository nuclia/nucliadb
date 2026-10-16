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

import json
import logging
from typing import Any

from nucliadb.common.catalog.interface import (
    Catalog,
    CatalogQuery,
)
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb_models.search import (
    CatalogFacetsRequest,
    ResourceResult,
    Resources,
    SortField,
    SortOptions,
    SortOrder,
)
from nucliadb_telemetry import metrics

search_observer = metrics.Observer("marklogic_catalog_search", labels={"op": ""})

logger = logging.getLogger(__name__)


def _search_javascript(sort: SortOptions | None, start: int, page_size: int) -> str:
    if sort is not None and sort.field == SortField.TITLE:
        sort_key = "String(root.title || '')"
    elif sort is not None and sort.field == SortField.MODIFIED:
        sort_key = "timestamp(root.modified_at)"
    else:
        # Deprecated order by score, use created_at instead
        sort_key = "timestamp(root.created_at)"
    descending = sort is None or sort.order == SortOrder.DESC
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})"
    # Without range indexes, sort keys are read from the documents matched by the universal index
    return f"""
function timestamp(value) {{
  // Date.parse supports up to millisecond precision
  const parsed = Date.parse(String(value || '').replace(/(\\.\\d{{3}})\\d+/, '$1'));
  return isNaN(parsed) ? 0 : parsed;
}}
const items = [];
for (const doc of cts.search({query}, ['unfiltered'])) {{
  const root = doc.root;
  items.push({{uri: xdmp.nodeUri(doc), key: {sort_key}, labels: root.facets ? root.facets.toObject() : []}});
}}
const direction = {-1 if descending else 1};
items.sort((a, b) => (a.key < b.key ? -direction : a.key > b.key ? direction : a.uri < b.uri ? -1 : a.uri > b.uri ? 1 : 0));
const result = {{
  total: items.length,
  results: items.slice({start}, {start + page_size}).map(item => ({{uri: item.uri, labels: item.labels}})),
}};
result;
"""


class MarkLogicCatalog(Catalog):
    @search_observer.wrap({"op": "search"})
    async def search(self, catalog_query: CatalogQuery) -> Resources:
        driver = get_driver()
        if not isinstance(driver, MarkLogicDriver):
            raise TypeError("MarkLogic catalog requires MarkLogicDriver")

        start = catalog_query.page_size * catalog_query.page_number
        javascript = _search_javascript(catalog_query.sort, start, catalog_query.page_size)
        async with driver.ro_transaction(kbid=catalog_query.kbid) as txn:
            response = await documents.evaluate(txn, javascript)
        # An empty response means the KnowledgeBox database does not exist
        result: dict[str, Any] = response[0] if response else {"total": 0, "results": []}

        total = int(result["total"])
        results = result["results"]
        return Resources(
            facets={},
            results=[
                ResourceResult(
                    rid=MarkLogicURIs.resource_id_from_uri(item["uri"]),
                    field="title",
                    field_type="a",
                    labels=[label for label in item["labels"] if label.startswith("/l/")],
                    score=0,
                )
                for item in results
            ],
            query=catalog_query.query.query if catalog_query.query else "",
            total=total,
            page_number=catalog_query.page_number,
            page_size=catalog_query.page_size,
            next_page=start + len(results) < total,
            min_score=0,
        )

    @search_observer.wrap({"op": "catalog_facets"})
    async def facets(self, kbid: str, request: CatalogFacetsRequest) -> dict[str, int]:
        # TODO(Marklogic)
        return {}
