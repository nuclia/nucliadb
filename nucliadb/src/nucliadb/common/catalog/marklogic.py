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

import datetime
import json
import logging
import re
from typing import Any

from nucliadb.common.catalog.interface import (
    Catalog,
    CatalogExpression,
    CatalogQuery,
)
from nucliadb.common.exceptions import InvalidQueryError
from nucliadb.common.maindb.collections import MarkLogicCollections
from nucliadb.common.maindb.marklogic import MarkLogicDriver
from nucliadb.common.maindb.utils import get_driver
from nucliadb.common.marklogic import documents
from nucliadb.common.marklogic.uris import MarkLogicURIs
from nucliadb_models import search as search_models
from nucliadb_models.labels import translate_alias_to_system_label, translate_system_to_alias_label
from nucliadb_models.search import (
    CatalogFacetsRequest,
    ResourceResult,
    Resources,
    SortField,
    SortOrder,
)
from nucliadb_telemetry import metrics

search_observer = metrics.Observer("marklogic_catalog_search", labels={"op": ""})

logger = logging.getLogger(__name__)

# Mirrors the resource-level labels generated for the index (see ResourceBrain._set_resource_labels),
# plus the computed field classifications, as the catalog used to store them.
_RESOURCE_LABELS_JS = """
function resourceLabels(root) {
  const basic = root.basic || {};
  const origin = root.origin || null;
  const labels = new Set();
  if (origin) {
    if (origin.source_id) {
      labels.add('/o/' + origin.source_id);
      labels.add('/u/s/' + origin.source_id);
    }
    for (const tag of origin.tags || []) labels.add('/t/' + tag);
    if (origin.path) labels.add('/p/' + origin.path.replace(/^\\/+/, ''));
    for (const collaborator of origin.colaborators || []) labels.add('/u/o/' + collaborator);
    for (const [key, value] of Object.entries(origin.metadata || {})) {
      labels.add('/m/' + key.substring(0, 255) + '/' + String(value).substring(0, 255));
    }
  }
  labels.add('/n/i/' + (basic.icon || ''));
  const metadata = basic.metadata || {};
  labels.add('/n/s/' + (metadata.useful ? (metadata.status || 'PENDING') : 'EMPTY'));
  if (metadata.language) labels.add('/s/p/' + metadata.language);
  for (const language of metadata.languages || []) labels.add('/s/s/' + language);
  const cancelled = new Set();
  for (const classification of (basic.usermetadata || {}).classifications || []) {
    const label = '/l/' + classification.labelset + '/' + classification.label;
    if (classification.cancelled_by_user) cancelled.add(label);
    else labels.add(label);
  }
  for (const fieldClassifications of (basic.computedmetadata || {}).field_classifications || []) {
    for (const classification of fieldClassifications.classifications || []) {
      const label = '/l/' + classification.labelset + '/' + classification.label;
      if (!cancelled.has(label)) labels.add(label);
    }
  }
  if (basic.hidden) labels.add('/q/h');
  return Array.from(labels);
}
function expandFacets(labels) {
  const facets = new Set();
  for (const label of labels) {
    const parts = label.split('/');
    for (let i = 2; i <= parts.length; i++) facets.add(parts.slice(0, i).join('/'));
  }
  return facets;
}
"""

# Approximates pg_trgm word_similarity, which the catalog used for fuzzy title matches
_TEXT_QUERY_JS = """
function trigrams(text) {
  const result = [];
  for (const word of text.toLowerCase().split(/[^\\p{L}\\p{N}]+/u)) {
    if (!word) continue;
    const padded = '  ' + word + ' ';
    for (let i = 0; i + 3 <= padded.length; i++) result.push(padded.substring(i, i + 3));
  }
  return result;
}
function wordSimilarity(query, text) {
  const queryTrigrams = new Set(trigrams(query));
  if (queryTrigrams.size === 0) return 0;
  const textTrigrams = trigrams(text);
  let best = 0;
  for (let start = 0; start < textTrigrams.length; start++) {
    if (!queryTrigrams.has(textTrigrams[start])) continue;
    const extent = new Set();
    let shared = 0;
    for (let end = start; end < textTrigrams.length; end++) {
      if (!extent.has(textTrigrams[end])) {
        extent.add(textTrigrams[end]);
        if (queryTrigrams.has(textTrigrams[end])) shared++;
      }
      best = Math.max(best, shared / (queryTrigrams.size + extent.size - shared));
    }
  }
  return best;
}
function matchesText(query, root) {
  if (query === null) return true;
  const value = String(root[query.field] || '');
  switch (query.match) {
    case 'exact': return value === query.query;
    case 'starts_with':
      return query.field === 'slug' ? value.startsWith(query.query) : value.toLowerCase().startsWith(query.query.toLowerCase());
    case 'words': {
      const words = new Set(value.toLowerCase().split(/[^\\p{L}\\p{N}_]+/u));
      return query.words.every(word => words.has(word));
    }
    case 'fuzzy': return wordSimilarity(query.query, value) >= FUZZY_THRESHOLD;
    case 'ends_with': return value.toLowerCase().endsWith(query.query.toLowerCase());
    case 'contains': return value.toLowerCase().includes(query.query.toLowerCase());
  }
  return false;
}
"""

SPLIT_REGEX = re.compile(r"\W")

# Same default as pg_trgm.word_similarity_threshold
FUZZY_THRESHOLD = 0.6


def _text_query_data(query: search_models.CatalogQuery | None) -> dict[str, Any] | None:
    if query is None or not query.query:
        return None
    return {
        "field": query.field.value,
        "match": query.match.value,
        "query": query.query,
        "words": [word.lower() for word in SPLIT_REGEX.split(query.query) if word],
    }


def _epoch_millis(value: datetime.datetime) -> float:
    # Naive datetimes are UTC, as the protobuf timestamps stored in the resources
    if value.tzinfo is None:
        value = value.replace(tzinfo=datetime.timezone.utc)
    return value.timestamp() * 1000


def _filter_data(expr: CatalogExpression) -> dict[str, Any]:
    if expr.bool_and is not None:
        return {"and": [_filter_data(operand) for operand in expr.bool_and]}
    elif expr.bool_or is not None:
        return {"or": [_filter_data(operand) for operand in expr.bool_or]}
    elif expr.bool_not is not None:
        return {"not": _filter_data(expr.bool_not)}
    elif expr.date is not None:
        return {
            "date": {
                "field": expr.date.field,
                "since": _epoch_millis(expr.date.since) if expr.date.since else None,
                "until": _epoch_millis(expr.date.until) if expr.date.until else None,
            }
        }
    elif expr.resource_id is not None:
        return {"resource_id": expr.resource_id}
    elif expr.facet is not None:
        return {"facet": expr.facet}
    else:
        raise NotImplementedError(f"Unsupported catalog filter: {expr}")


def _search_javascript(catalog_query: CatalogQuery, start: int, faceted: list[str]) -> str:
    sort = catalog_query.sort
    if sort is not None and sort.field == SortField.TITLE:
        sort_key = "String(root.title || '')"
    elif sort is not None and sort.field == SortField.MODIFIED:
        sort_key = "timestamp(root.modified_at)"
    else:
        # Deprecated order by score, use created_at instead
        sort_key = "timestamp(root.created_at)"
    descending = sort is None or sort.order == SortOrder.DESC
    filters = _filter_data(catalog_query.filters) if catalog_query.filters else None
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})"
    # Without range indexes, sort keys are read from the documents matched by the universal index
    return f"""
{_RESOURCE_LABELS_JS}
{_TEXT_QUERY_JS}
const FUZZY_THRESHOLD = {FUZZY_THRESHOLD};
const textQuery = {json.dumps(_text_query_data(catalog_query.query))};
function timestamp(value) {{
  // Date.parse supports up to millisecond precision
  const parsed = Date.parse(String(value || '').replace(/(\\.\\d{{3}})\\d+/, '$1'));
  return isNaN(parsed) ? 0 : parsed;
}}
function matches(expr, root, rid, facets) {{
  if (expr === null) return true;
  if (expr.and) return expr.and.every(item => matches(item, root, rid, facets));
  if (expr.or) return expr.or.some(item => matches(item, root, rid, facets));
  if (expr.not) return !matches(expr.not, root, rid, facets);
  if (expr.resource_id !== undefined) return rid === expr.resource_id;
  if (expr.facet !== undefined) return facets.has(expr.facet);
  if (expr.date) {{
    const value = timestamp(root[expr.date.field]);
    if (expr.date.since !== null && expr.date.until !== null) return value >= expr.date.since && value <= expr.date.until;
    if (expr.date.since !== null) return value > expr.date.since;
    return value < expr.date.until;
  }}
  return false;
}}
const filters = {json.dumps(filters)};
const faceted = {json.dumps(faceted)};
const facetCounts = {{}};
for (const prefix of faceted) facetCounts[prefix] = {{}};
const items = [];
for (const doc of cts.search({query}, ['unfiltered'])) {{
  const root = doc.root.toObject();
  if (!matchesText(textQuery, root)) continue;
  const uri = xdmp.nodeUri(doc);
  const labels = resourceLabels(root);
  const facets = expandFacets(labels);
  if (!matches(filters, root, uri.split('/').pop().replace('.json', ''), facets)) continue;
  for (const facet of facets) {{
    const parent = facet.substring(0, facet.lastIndexOf('/'));
    if (Object.prototype.hasOwnProperty.call(facetCounts, parent)) {{
      facetCounts[parent][facet] = (facetCounts[parent][facet] || 0) + 1;
    }}
  }}
  items.push({{uri: uri, key: {sort_key}, labels: labels}});
}}
const direction = {-1 if descending else 1};
items.sort((a, b) => (a.key < b.key ? -direction : a.key > b.key ? direction : a.uri < b.uri ? -1 : a.uri > b.uri ? 1 : 0));
const result = {{
  total: items.length,
  results: items.slice({start}, {start + catalog_query.page_size}).map(item => ({{uri: item.uri, labels: item.labels}})),
  facets: facetCounts,
}};
result;
"""


def _facets_javascript(request: CatalogFacetsRequest) -> str:
    prefixes = [{"prefix": prefix.prefix, "depth": prefix.depth} for prefix in request.prefixes]
    query = f"cts.collectionQuery({json.dumps(MarkLogicCollections.RESOURCES)})"
    return f"""
{_RESOURCE_LABELS_JS}
const prefixes = {json.dumps(prefixes)};
const depthOf = facet => facet.split('/').length;
function selected(facet) {{
  if (prefixes.length === 0) return true;
  return prefixes.some(item =>
    (facet === item.prefix || facet.startsWith(item.prefix + '/')) &&
    (item.depth === null || depthOf(facet) - depthOf(item.prefix) <= item.depth)
  );
}}
const counts = {{}};
for (const doc of cts.search({query}, ['unfiltered'])) {{
  for (const facet of expandFacets(resourceLabels(doc.root.toObject()))) {{
    if (selected(facet)) counts[facet] = (counts[facet] || 0) + 1;
  }}
}}
counts;
"""


async def _evaluate(kbid: str, javascript: str) -> dict[str, Any] | None:
    driver = get_driver()
    if not isinstance(driver, MarkLogicDriver):
        raise TypeError("MarkLogic catalog requires MarkLogicDriver")
    async with driver.ro_transaction(kbid=kbid) as txn:
        response = await documents.evaluate(txn, javascript)
    # An empty response means the KnowledgeBox database does not exist
    return response[0] if response else None


class MarkLogicCatalog(Catalog):
    @search_observer.wrap({"op": "search"})
    async def search(self, catalog_query: CatalogQuery) -> Resources:
        start = catalog_query.page_size * catalog_query.page_number
        faceted = [translate_label(facet) for facet in catalog_query.faceted]
        javascript = _search_javascript(catalog_query, start, faceted)
        result = await _evaluate(catalog_query.kbid, javascript) or {
            "total": 0,
            "results": [],
            "facets": {facet: {} for facet in faceted},
        }

        facets = {
            translate_system_to_alias_label(prefix): {
                translate_system_to_alias_label(facet): count for facet, count in counts.items()
            }
            for prefix, counts in result["facets"].items()
        }
        total = int(result["total"])
        results = result["results"]
        return Resources(
            facets=facets,
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
        result = await _evaluate(kbid, _facets_javascript(request)) or {}
        return {facet: int(count) for facet, count in result.items()}


def translate_label(literal: str) -> str:
    if len(literal) == 0:
        raise InvalidQueryError("filters", "Invalid empty label")
    if literal[0] != "/":
        raise InvalidQueryError("filters", f"Invalid label. It must start with a `/`: {literal}")
    return translate_alias_to_system_label(literal)
