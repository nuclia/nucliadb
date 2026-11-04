(() => {
  // Evaluate field/resource predicates on processed fields, which carry field
  // metadata and copied resource metadata, rather than on every child chunk.
  const scope = cts.andQuery([
    cts.collectionQuery(COLLECTIONS.fieldsProcessed),
    fieldSecurityQuery
  ]);
  // Predicates such as text/phrases need filtered document verification; exact
  // index-resolvable predicates can enumerate matches through the URI lexicon.
  const uris = matchingUris(cts.andQuery([scope, ${query}]), ${filtered});
  // Avoid a large directory list when all allowed fields appear to match.
  // cts.estimate is an index estimate, not a verified document count. Preserve
  // [] for no matches; null is the sentinel for no remaining field restriction.
  if (uris.length && uris.length >= cts.estimate(scope)) return null;
  // Removing the processed-document suffix leaves a trailing-slash field URI
  // prefix. Recursive directory queries then include its chunks and embeddings.
  return uris.map(uri => uri.replace(/processed\.json$$/, ''));
})()