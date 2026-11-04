(() => {
  // Stage 1: Translate paragraph predicates from chunk documents to an embedding
  // acceptance test. Skip this work for a zero limit or an unrestricted OR field
  // side, since that side already admits every otherwise eligible embedding.
  let accept = null;
  if (${filter_limit} && paragraphQuery !== null && !(paragraphOr && fieldQuery === null)) {
    // AND can narrow chunks by field first. OR must find paragraph matches
    // outside those fields too; security remains common to both alternatives.
    const chunkQuery = cts.andQuery([
      cts.collectionQuery(COLLECTIONS.chunks),
      securityQuery,
      ...(!paragraphOr && fieldQuery !== null ? [fieldQuery] : []),
      paragraphQuery
    ]);
    // Chunk and embedding identifiers differ, so join by field, optional split
    // and paragraph offsets. Filtered URI lookup verifies paragraph predicates
    // when required. For OR, union these keys with whole matching fields.
    const keys = new Set(matchingUris(chunkQuery, ${paragraph_filtered}).map(chunkKey));
    const fields = paragraphOr ? new Set(fieldDirs) : null;
    accept = root => (fields !== null && fields.has(fieldDir(root))) || keys.has(embeddingKey(root));
  }
  // Stage 2: Search only embedding documents for this model. embeddingScope
  // includes fields for AND, but deliberately does not reapply them for OR.
  // The accept callback supplies paragraph-level checks before cosine scoring.
  const model = ${model};
  const scope = cts.andQuery([
    cts.collectionQuery(COLLECTIONS.embeddings),
    embeddingScope,
    cts.jsonPropertyValueQuery('model_id', model, ['exact'])
  ]);
  // Stage 3: The caller selects an ANN view per model and vector dimension.
  // The helper chooses exact versus ANN using the scoped index estimate, then
  // applies the same acceptance, similarity threshold and final limit to both.
  return semanticSearch({
    limit: ${limit},
    scope,
    accept,
    queryVector: vec.vector(${vector}),
    minScore: ${min_score},
    view: ${view},
    exactMax: ${exact_max},
    oversampling: ${oversampling}
  });
})()