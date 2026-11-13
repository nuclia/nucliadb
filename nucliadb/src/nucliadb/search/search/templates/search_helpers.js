// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//
// This helper source is loaded raw, not template-substituted. It shares the
// surrounding eval's constants and records the semantic branch's chosen mode.
let semanticMode = null;

// Directories are trailing-slash URI prefixes, not metadata field comparisons.
// Recursive matching includes descendants; an empty list must match nothing.
function dirsQuery(dirs) {
  return dirs.length ? cts.directoryQuery(dirs, 'infinity') : cts.falseQuery();
}

// URI-lexicon enumeration trusts index matches without document verification.
// Use filtered search for predicates whose index resolution may be inexact,
// such as phrases, and return the verified documents' URIs instead.
function matchingUris(query, filtered) {
  if (!filtered) return cts.uris('', [], query).toArray().map(String);
  const uris = [];
  for (const doc of cts.search(query, ['filtered', 'score-zero'])) {
    uris.push(String(xdmp.nodeUri(doc)));
  }
  return uris;
}

// Facets are hierarchical: include the exact label and its slash-delimited
// descendants. Walk the ordered range lexicon from that prefix and stop once
// values leave it; the slash boundary avoids matching unrelated sibling names.
function facetValues(path, facet) {
  const values = [facet];
  const prefix = facet + '/';
  const reference = cts.pathReference(path, ['type=string', 'collation=' + COLLATION]);
  for (const value of cts.values(reference, prefix)) {
    const text = String(value);
    if (!text.startsWith(prefix)) break;
    values.push(text);
  }
  return values;
}

// Range equality with the expanded values admits any descendant, not just the
// parent facet string. This is index-based hierarchy expansion, not a tree read.
function facetQuery(facet) {
  return cts.pathRangeQuery(PATHS.facets, '=', facetValues(PATHS.facets, facet));
}

// Processed fields hold copied resource facets as well as their own facets.
// A field-level facet predicate accepts either source, including descendants.
function fieldFacetQuery(facet) {
  const resourceFacets = facetValues(PATHS.resourceFacets, facet);
  return cts.orQuery([facetQuery(facet), cts.pathRangeQuery(PATHS.resourceFacets, '=', resourceFacets)]);
}

// Resolve a partial field id through processed-document URIs, then turn those
// exact fields into recursive directory scopes. The literal prefix check guards
// the URI-pattern lookup; child documents are selected by URI layout afterward.
function fieldPrefixQuery(base, prefix) {
  const start = base + prefix;
  const uris = cts.uriMatch(start + '*/processed.json').toArray().map(String)
    .filter(uri => uri.startsWith(start));
  return dirsQuery(uris.map(uri => uri.replace(/processed\.json$/, '')));
}

// Legacy security uses the indexed access-groups JSON path: nonempty groups
// make a document restricted; any caller-group overlap admits it. With no caller
// groups, every restricted document is denied, while public documents remain.
// This does not implement native document permissions. The Python hybrid path
// currently disables this filtering by supplying null rather than a group list.
function deniedQuery(groups) {
  const restricted = cts.pathRangeQuery(PATHS.accessGroups, '>', '');
  if (!groups.length) return restricted;
  return cts.andQuery([restricted, cts.notQuery(cts.pathRangeQuery(PATHS.accessGroups, '=', groups))]);
}

// Evaluate denials on resource documents and convert their document URIs to
// subtree prefixes, so one exclusion can cover all descendant search documents.
function deniedResources(groups) {
  const query = cts.andQuery([cts.collectionQuery(COLLECTIONS.resources), deniedQuery(groups)]);
  return cts.uris('', [], query).toArray().map(uri => String(uri).replace(/\.json$/, '/'));
}

// Strip the chunk storage wrapper but preserve its optional split and start-end
// offsets. The resulting field/split/offset key identifies a paragraph, rather
// than a particular vector window; unrelated URI shapes cannot produce a key.
function chunkKey(uri) {
  const match = /^(\/resources\/[^\/]+\/fields\/[^\/]+\/[^\/]+\/)chunks\/(.+)\/data\.json$/.exec(uri);
  return match ? match[1] + match[2] : null;
}

function matchingKeyPlan(query, filtered, column) {
  const op = require('/MarkLogic/optic');
  const source = filtered ? matchingUris(query, true) : query;
  const pattern = column === 'field_key' ? 'processed\\.json$' :
    '^(/resources/[^/]+/fields/[^/]+/[^/]+/)chunks/(.+)/data\\.json$';
  const replacement = column === 'field_key' ? '' : '$1$2';
  const qualifier = column === 'field_key' ? 'fields' : 'paragraphs';
  const uri = op.viewCol(qualifier, 'uri');
  let plan = op.fromDocUris(source, qualifier);
  if (column === 'paragraph_key') plan = plan.where(op.fn.matches(uri, pattern));
  return plan.select([op.as(column, op.fn.replace(uri, pattern, replacement))])
    .whereDistinct();
}

function semanticSearch(search) {
  semanticMode = 'exact';
  if (!search.limit) return [];

  function ranked(k) {
    const op = require('/MarkLogic/optic');
    // Field and paragraph keys are projected by TDE, so eligibility is decided
    // on rows before scoring rather than after loading embedding documents.
    const join = (plan, keys, column, qualifier = 'vectors') => {
      const matchColumn = 'matched_' + column;
      const matches = keys.select([op.as(matchColumn, op.col(column))]);
      return plan.joinInner(matches, op.on(op.viewCol(qualifier, column), op.col(matchColumn)));
    };
    const base = op.fromView(VECTOR_SCHEMA, search.view, 'vectors', op.fragmentIdCol('fid')).where(search.scope);
    let plan = base;
    if (search.paragraphOr && search.fieldPlan !== null && search.paragraphPlan !== null) {
      const fields = op.fromView(VECTOR_SCHEMA, search.view, 'field_vectors', op.fragmentIdCol('field_fid'))
        .where(search.scope);
      const paragraphs = op.fromView(VECTOR_SCHEMA, search.view, 'paragraph_vectors', op.fragmentIdCol('paragraph_fid'))
        .where(search.scope);
      const eligible = join(fields, search.fieldPlan, 'field_key', 'field_vectors')
        .select([op.as('eligible_fid', op.col('field_fid'))])
        .union(join(paragraphs, search.paragraphPlan, 'paragraph_key', 'paragraph_vectors')
          .select([op.as('eligible_fid', op.col('paragraph_fid'))])).whereDistinct();
      plan = base.joinInner(eligible, op.on(op.col('fid'), op.col('eligible_fid')));
    } else if (!(search.paragraphOr && search.fieldPlan === null && search.paragraphPlan !== null)) {
      if (search.fieldPlan !== null) plan = join(plan, search.fieldPlan, 'field_key');
      if (search.paragraphPlan !== null) plan = join(plan, search.paragraphPlan, 'paragraph_key');
    }
    // Bound the joined eligibility probe, without hydrating documents or counting the full scope.
    const eligibleSize = plan.select(['fid']).limit(search.exactMax + 1).result().toArray().length;
    if (!eligibleSize) return [];
    semanticMode = eligibleSize <= search.exactMax ? 'exact' : 'ann';
    const options = {maxDistance: 1 - search.minScore};
    if (semanticMode === 'exact') options.noIndex = null;
    // ANN is allowed, not forced; physical ANN-index prefilter correctness remains unverified.
    const rows = plan.annTopK(k, op.viewCol('vectors', 'vector'), search.queryVector, op.col('distance'), options)
      .joinDocUri(op.col('uri'), op.fragmentIdCol('fid'))
      .select(['uri', 'distance']).result().toArray();
    return rows.map(row => ({score: 1 - Number(row.distance), uri: String(row.uri)}));
  }

  // The TDE view must exist and be queryable; view errors propagate to the caller.
  // One scoring call for every scope size; short pages do not trigger retries.
  const scored = ranked(search.limit);
  scored.sort((left, right) => right.score - left.score);
  // Optic already selected the winners and computed their distances; only the
  // limited winning documents need hydration for response metadata.
  return scored.slice(0, search.limit).map(result => {
    const root = cts.doc(result.uri).root;
    return {score: result.score, content: {chunk_id: root.chunk_id, facets: root.facets, data: root.data}};
  });
}