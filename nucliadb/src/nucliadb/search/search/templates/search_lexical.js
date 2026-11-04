(() => {
  // This branch returns at most the requested number of chunks, not a total
  // hit count. A zero limit avoids both the exact and fuzzy searches.
  const limit = ${limit};
  const minScore = ${min_score};
  if (!limit) return [];

  function run(textQuery, fuzzy, boost) {
    const results = [];
    // Restrict BM25 retrieval to chunk documents and the combined hybrid scope.
    // BM25 options preserve descending score order, which the cutoff relies on.
    const search = cts.search(
      cts.andQuery([cts.collectionQuery(COLLECTIONS.chunks), chunkScope, textQuery]),
      ${bm25_options}
    );
    for (const doc of search) {
      // Apply the branch boost before testing the threshold. Descending scores
      // let us stop at the first rejection instead of scanning the remaining hits.
      const score = cts.score(doc) * boost;
      if (score < minScore) break;
      // Hydrate only accepted hits and retain the payload needed by the caller.
      const content = doc.root.toObject();
      results.push({
        content: {chunk_id: content.chunk_id, facets: content.facets, data: content.data},
        fuzzy,
        score
      });
      if (results.length >= limit) break;
    }
    return results;
  }

  // The supplied array of words/phrases uses MarkLogic's default OR semantics:
  // it is not an implicit AND across all input terms. Exact here means the
  // non-fuzzy, unstemmed, unwildcarded word query, not literal string equality.
  const exact = run(cts.jsonPropertyWordQuery('text', ${exact_terms}, ${word_options}), false, 1);
  // Python prepares one-edit variants for eligible standalone words (not quoted
  // phrases). Fuzzy retrieval is fallback-only: it neither fills a short exact
  // page nor runs when a nonzero minimum BM25 score was requested.
  const patterns = ${patterns};
  if (exact.length || minScore !== 0 || !patterns.length) return exact;
  let fuzzyQuery;
  try {
    // Expand wildcard variants through the text word lexicon, then search the
    // resulting words normally so they can receive meaningful BM25 scores.
    const words = patterns.flatMap(pattern =>
      cts.jsonPropertyWordMatch('text', pattern, ${lexicon_options}).toArray().map(String));
    fuzzyQuery = words.length ? cts.jsonPropertyWordQuery('text', words, ${word_options}) : null;
  } catch (error) {
    // Only a missing lexicon permits the direct wildcard fallback. Those
    // wildcard matches can score zero; the zero-cutoff guard above allows them.
    // Other errors must surface rather than silently changing retrieval mode.
    if (!/LXCNNOTFOUND/.test(String(error.name) + String(error.message))) throw error;
    fuzzyQuery = cts.jsonPropertyWordQuery('text', patterns, ['case-insensitive', 'unstemmed', 'wildcarded']);
  }
  // The fuzzy boost lowers fallback scores relative to ordinary lexical hits.
  // An available lexicon with no expansions produces no fallback matches.
  return fuzzyQuery === null ? [] : run(fuzzyQuery, true, ${fuzzy_boost});
})()