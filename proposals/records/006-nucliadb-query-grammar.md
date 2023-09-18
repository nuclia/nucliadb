# NucliaDB Query Grammar

Based on a recent story, we need to change the query parsing behavior for
`/catalog` endpoint to allow prefix matching in the last word. `/catalog`
internally calls `Search` gRPC call to the node (same as `/search` endpoint).

As `/catalog` and `/search` use the same underlaying gRPC call, both need to be
able to choose a query parsing strategy.

## Current solution/situation

[Detailed description of problem to be solved]

Currently, we are not able to choose query parsing strategies with the current
`SearchRequest` protobuffer.

In addition, while working on this story, I've found we are parsing
`SearchRequest` `query` field directly with tantivy's `QueryParser` (in both
`nucliadb_paragraphs` and `nucliadb_texts`). That means our API users can make
use of it (although not intended). We use to have an `advanced_query` field to
expose tantivy's query grammar. However, as we are building the unified index,
we decided to remove this field to avoid users depending on it and forcing us to
support tantivy's query grammar.

As the unified index will need to support `/catalog` and `/search` semantics,
we'll need some way to use different query parsing strategies. And it'll be
interesting to not expose tantivy's query grammar anymore.

## Proposed Solution

I have 3 proposed solutions for this problem:

1. Modify `SearchRequest` protobuffer so we can know which query parsing we want
   and ignore tantivy query grammar exposition.
2. Implement a custom query parser on the node and add some field in
   `SearchRequest` protobuffer to choose it.
3. Implement a Query AST using protobuffers, moving the query parsing
   responsability to the Python side

### Solution 1: protobuf modification

Create a new field and/or deprecate the `document` field, so we can choose if we
want to search using `nucliadb_texts` or not and with which query parser.
Instead of a bool, we could create an enum with multiple options like:
`NO_DOC_SEARCH`, `TERMS_QUERY`, `LAST_WORD_PREFIX`...

The advantage is simplicity, we only need to add a new field and implement a
simple conditional logic.

However, we'll still be parsing tantivy query grammar in the `query` field.

### Solution 2: Rust query parser

Implementing a query parser in Rust replacing tantivy's one will solve the
tantivy grammar parsing issue. However, we'll need another field to know which
kind of parsing do we want or we'll need to make Python side aware of the query
grammar too.

To explain this, a simple example: the node receives this query "Ramon progra*".
It's the user using our query grammar or it's search component asking for a
prefix search? We'll need an extra field (as Solution 1) to know it.

### Solution 3: Implement query AST using protobuffers

Implementing the query AST in protobuffers would have a simple mapping in the
Rust side from the protobuf grammar to tantivy or unified index one. However,
we'll need to move query parsing logic to Python side.

Having Python create the query would make things easier for the Rust (as the
query comes already defined). In addition, the unified index could implement the
same and make both projects compatible. However, this is the most costly
solution as implies creating this query grammar and parsing code.

## Rollout plan

[Describe implementation plan]


## Success Criteria

[How will we know if the proposal was successful?]
