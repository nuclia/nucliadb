// Shared names come from Python's schema definitions, not request text. Each
// collection selects a document family: resources for resource/graph scoping,
// processed fields for prefilters, chunks for lexical/paragraph matching, and
// embeddings for semantic retrieval. Collection scoping is not authorization.
const COLLECTIONS = ${collections};
// Range-index paths address facets, copied resource facets and legacy access
// groups. They must agree with the indexed JSON paths used during ingestion.
const PATHS = ${paths};
// ANN views live in this schema; the vector branch selects a model/dimension
// view within it. Facet lexicon expansion uses the configured string collation.
const VECTOR_SCHEMA = ${vector_schema};
const COLLATION = ${collation};