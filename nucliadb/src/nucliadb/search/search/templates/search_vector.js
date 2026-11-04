(() => {
  let paragraphKeys = null;
  if (${filter_limit} && paragraphQuery !== null && !(paragraphOr && fieldQuery === null)) {
    // AND can narrow chunks by field first. OR must find paragraph matches
    // outside those fields too; security remains common to both alternatives.
    const chunkQuery = cts.andQuery([
      cts.collectionQuery(COLLECTIONS.chunks),
      securityQuery,
      ...(!paragraphOr && fieldQuery !== null ? [fieldQuery] : []),
      paragraphQuery
    ]);
    paragraphKeys = matchingUris(chunkQuery, ${paragraph_filtered}).map(chunkKey).filter(key => key !== null);
  }
  const model = ${model};
  const scope = cts.andQuery([
    cts.collectionQuery(COLLECTIONS.embeddings),
    embeddingScope,
    cts.jsonPropertyValueQuery('model_id', model, ['exact'])
  ]);
  return semanticSearch({
    limit: ${limit},
    scope,
    fieldKeys: fieldDirs,
    paragraphKeys,
    paragraphOr,
    unrestricted: fieldDirs === null && paragraphKeys === null && groups === null,
    queryVector: vec.vector(${vector}),
    minScore: ${min_score},
    view: ${view},
    exactMax: ${exact_max},
    oversampling: ${oversampling}
  });
})()