(() => {
  // Stage 1: Install the shared index names and helpers in this eval's scope.
  // The lexical, vector and optional graph branches reuse the same prefilter work.
  ${constants}
  ${helpers}

  // Stage 2: Resolve field matches and any security restriction not already
  // represented by those matches. Graph retrieval gets a resource-level scope.
  // HYBRID SECURITY IS CURRENTLY DISABLED by the Python caller, which supplies
  // null groups; these templates retain the legacy group-filtering machinery.
  ${prefilter}
  ${graph_resource_scope}

  // Stage 3: An empty field selection makes an AND search impossible. Do not
  // short-circuit OR searches, whose paragraph side may still match, or graph
  // searches, whose resource scope can match even when no processed fields do.
  const paragraphOr = ${paragraph_or};
  if (fieldDirs !== null && !fieldDirs.length && !paragraphOr && ${no_graph}) {
    return {chunks: [], embeddings: []};
  }
  // Stage 4: null fieldDirs means no field restriction; [] means match nothing.
  // A null fieldQuery is thus an absent predicate, not a false query. For AND,
  // intersect field and paragraph matches; for OR, union them. An unrestricted
  // field side makes that union unrestricted, even when paragraphs are filtered.
  const paragraphQuery = ${paragraph_query};
  const fieldQuery = fieldDirs === null ? null : dirsQuery(fieldDirs);
  const filterQuery = paragraphQuery === null ? fieldQuery :
    fieldQuery === null ? (paragraphOr ? null : paragraphQuery) :
    paragraphOr ? cts.orQuery([fieldQuery, paragraphQuery]) : cts.andQuery([fieldQuery, paragraphQuery]);
  // Chunk predicates apply directly to chunk documents. Embeddings cannot use
  // paragraph text/facet predicates directly: the vector branch maps matching
  // chunks to embedding keys. With OR, do not reapply the field prefilter here,
  // since it would discard matches admitted only by the paragraph side.
  const chunkScope = filterQuery === null ? securityQuery : cts.andQuery([securityQuery, filterQuery]);
  const embeddingScope = !paragraphOr && fieldQuery !== null
    ? cts.andQuery([securityQuery, fieldQuery]) : securityQuery;

  // Stage 5: Execute the enabled branches and return separate result families.
  // Scores are not fused here. The vector helper sets semanticMode to report
  // whether the returned semantic candidates came from ANN or an exact scan.
  return {
    chunks: ${chunks},
    embeddings: ${embeddings},
    graph: ${graph},
    semantic_mode: semanticMode
  };
})()