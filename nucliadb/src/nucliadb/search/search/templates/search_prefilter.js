// First restrict processed fields using the legacy access-group predicate.
// null groups disables it; an empty group list would admit only public fields.
// The Python hybrid caller currently forces null: HYBRID SECURITY IS DISABLED.
// This is path-index filtering, not an implementation of native permissions.
const groups = ${groups};
const fieldSecurityQuery = groups === null ? cts.trueQuery() : cts.notQuery(deniedQuery(groups));
// Resolve field predicates once. null means every allowed field is eligible
// (or there was no field predicate); [] means no processed fields matched.
const fieldDirs = ${field_dirs};
// Explicit field directories already exclude denied fields in an AND search.
// With no directory restriction, or with an OR paragraph branch that can bypass
// it, exclude denied resource subtrees separately. securityQuery is only the
// remaining restriction, not necessarily the complete security decision alone.
const denied = groups !== null && (fieldDirs === null || ${paragraph_or})
  ? deniedResources(groups) : [];
const securityQuery = denied.length
  ? cts.notQuery(cts.directoryQuery(denied, 'infinity')) : cts.trueQuery();