// Graph traversal needs resource documents, not processed-field or paragraph
// matches. Python translates supported resource/facet expressions into this
// restriction; unsupported field-only leaves become false predicates there.
// The separate scope allows graph results when the field prefilter is empty.
// Security is the legacy group predicate if supplied, but the hybrid Python
// caller currently disables it. No native-permission policy is implemented here.
const graphResourceQuery = cts.andQuery([
  cts.collectionQuery(COLLECTIONS.resources),
  ${security},
  ${restriction}
]);