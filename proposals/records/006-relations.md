# Entity Relation Indexing and Graph

The purpose of this proposal is to describe a short to long term strategy for our Entity Relation Indexing
and Graph capabilities.


## Current solution/situation

The current implementation of relations is done through the `nucliadb_relations` rust package.


What are "Relations"?:
- detected entities
- resources
- labels
- field/paragraphs


REST API Integration:
- Find/Search
    - `features`: `relations` is a search option
        - When enabled, we will call NUA `/tokens` endpoint to pull out NER values
        - these are then injected into the search GRPC request `relation_subgraph` values using depth 1
        - Returned as separate part of the result
- Suggest
    - allow searching relations by prefix
- Entity Management
    - used a lot in `entitiesgroups` API
    - get entities
    - index/merge entities: `JoinGraph`
        - on changes, need to update all nodes

GRPC Interfaces:
    - RelationSearch: used in entities storage implementation
    - RelationTypes: used in entities storage implementation
    - RelationEdges: not used anywhere
    - RelationIds: not used anywhere
    - JoinGraph: used in entities storage/indexing


Problems:
- Graph storage implementation is difficult to scale
- Lacks cohesive with other index types
- orm/entities.py implementation has coupling with tikv and index storage state for global entity management
- Global entity changes require writes against all nodes and can cause some inconsistencies
- Current implementation does not work well with large datasets
- Inconsistent relation search results
- Should relation lookups be part of the search request or a separate endpoint?
- Not shown/used with chat

## Proposed Solution

The proposal is to adjust our focus on the graph implementation. Instead of trying to
integrate a complete relational graph, implement relation lookup features that we need
for the product short term that also integrate with the unified index.


### Short term

Clean up the interfaces and remove functionality that is not providing customer value and
that makes it more difficult for us to move forward:

- Remove `RelationEdges` and `RelationIds` endpoints: unused right now, remove
- Remove feature of adding/removing entities globally
- Remove `JoinGraph` as they will be unused after removing above feature
- Remove relation search results at search/find/chat
- Add new endpoint to lookup relations


### Medium term

Implement basic relation lookups with unifieid index implementation.

Relation Fields:
- from
- from_type
- to
- to_type
- type: (CHILD, ABOUT, ENTITY, OTHER, etc -- only OTHER currently used)

Investigate how tantivy does faceted search for potential implementation details.


### Long term

Long term, as the use case is more clear, implement a full relational graph outside of the unified index.

This is something where indexing times can be slower and we have less real time constraints.


## Resources

- https://github.com/RManLuo/Awesome-LLM-KG