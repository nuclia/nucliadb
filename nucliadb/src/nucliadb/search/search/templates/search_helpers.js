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

// Embedding metadata supplies the parent field prefix used by directory scopes
// and by the paragraph-to-embedding join. Field ids do not contain slashes.
function fieldDir(root) {
  return '/resources/' + root.resource_id + '/fields/' + root.field_type + '/' + root.field_id + '/';
}

// Strip the chunk storage wrapper but preserve its optional split and start-end
// offsets. The resulting field/split/offset key identifies a paragraph, rather
// than a particular vector window; unrelated URI shapes cannot produce a key.
function chunkKey(uri) {
  const match = /^(\/resources\/[^\/]+\/fields\/[^\/]+\/[^\/]+\/)chunks\/(.+)\/data\.json$/.exec(uri);
  return match ? match[1] + match[2] : null;
}

function embeddingKey(root) {
  // Vector ids are rid/field_type/field_id/[split/]index/start-end. Preserve the
  // optional split, discard the vector index, and use data.position's paragraph
  // offsets (not the vector window's trailing offsets) to match chunkKey.
  const parts = String(root.chunk_id).split('/');
  const split = parts.length === 6 ? parts[3] + '/' : '';
  const position = root.data && root.data.position;
  return position ? fieldDir(root) + split + position.start + '-' + position.end : null;
}

function semanticSearch(search) {
  // Stage 1: Exact is the default, including zero-limit and fallback cases.
  // Both paths load stored vectors and recompute cosine similarity so returned
  // scores and threshold decisions use the same calculation, not ANN distance.
  semanticMode = 'exact';
  if (!search.limit) return [];
  const similarity = root => 1 - vec.cosineDistance(vec.vector(root.vector), search.queryVector);
  const keep = (scored, root) => {
    // Paragraph acceptance runs before the more expensive vector conversion and
    // cosine calculation. Hydration has already happened in either caller path.
    if (search.accept !== null && !search.accept(root)) return;
    const score = similarity(root);
    if (score >= search.minScore) scored.push({score, root});
  };

  function exact() {
    const scored = [];
    // Stage 2a: Scan scoped index candidates without text relevance scoring.
    // "Exact" means exhaustive cosine scoring of those candidates, not document
    // verification: unfiltered search trusts index resolution of the scope.
    for (const doc of cts.search(search.scope, ['unfiltered', 'score-zero'])) {
      const root = doc.root.toObject();
      keep(scored, root);
    }
    return scored;
  }

  // Stage 2b: ANN chooses candidates from the model/dimension view first.
  // The following where is a postfilter, not an ANN prefilter: collection,
  // model and directory restrictions can remove nearest neighbors afterward.
  // Oversampling compensates for that loss, but does not guarantee full recall.
  function ann(k) {
    const op = require('/MarkLogic/optic');
    const rows = op.fromView(VECTOR_SCHEMA, search.view, '', op.fragmentIdCol('fid'))
      .annTopK(k, op.col('vector'), search.queryVector, op.col('distance'), {maxDistance: 1 - search.minScore})
      .where(search.scope)
      .joinDocUri(op.col('uri'), op.fragmentIdCol('fid'))
      .select(['uri']).result().toArray();
    // Join fragment ids back to document URIs and hydrate surviving embeddings.
    // The projected ANN distance is deliberately not returned: keep recomputes
    // cosine from each stored vector and applies paragraph/score acceptance.
    const uris = rows.map(row => String(row.uri));
    const scored = [];
    for (const uri of uris) keep(scored, cts.doc(uri).toObject());
    return scored;
  }

  let scored = null;
  // Stage 3: Estimate scoped candidates from indexes to choose the cheaper path.
  // cts.estimate is only an estimate, not a verified count or returned-hit count.
  if (cts.estimate(search.scope) > search.exactMax) {
    try {
      // Try a minimum of 100 or limit times oversampling, then eight times that
      // base. Count only candidates surviving scope, paragraph and score checks;
      // if neither attempt fills the requested page, fall back to an exact scan.
      const base = Math.max(search.limit * search.oversampling, 100);
      for (const k of [base, base * 8]) {
        const result = ann(k);
        if (result.length >= search.limit) {
          scored = result;
          semanticMode = 'ann';
          break;
        }
      }
    } catch (error) {
      // A model/dimension view may not exist yet or may still be reindexing.
      // Only those table errors trigger exact fallback; all other errors surface.
      if (!/SQL-TABLE(NOTFOUND|REINDEXING)/.test(String(error.name) + String(error.message))) throw error;
    }
  }
  // Stage 4: Small scopes, insufficient ANN survivors and the allowed view
  // failures all reach the exhaustive path. Finally sort by recomputed cosine,
  // slice to the limit, and expose only the result metadata the caller consumes.
  if (scored === null) scored = exact();
  scored.sort((left, right) => right.score - left.score);
  return scored.slice(0, search.limit).map(({score, root}) =>
    ({score, content: {chunk_id: root.chunk_id, facets: root.facets, data: root.data}}));
}