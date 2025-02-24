// Copyright (C) 2021 Bosutech XXI S.L.
//
// nucliadb is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at info@nuclia.com.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//
use std::collections::HashSet;
use std::fmt::Debug;
use std::path::Path;

use nidx_protos::relation_prefix_search_request::Search;
use nidx_protos::{
    EntitiesSubgraphResponse, RelationNode, RelationPrefixSearchResponse, RelationSearchRequest, RelationSearchResponse,
};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, FuzzyTermQuery, Occur, Query, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Index, IndexReader, Term};

use crate::graph_query_parser::{self, Expression, FuzzyTerm, GraphQuery, GraphQueryParser, GraphSearcher, Node, PathQuery, Relation};
use crate::schema::Schema;
use crate::{io_maps, schema};

const FUZZY_DISTANCE: u8 = 1;
// Search for entities of these many words of length
const ENTITY_WORD_SIZE: usize = 3;
const NUMBER_OF_RESULTS_SUGGEST: usize = 10;
// Hard limit until we have pagination in place
const MAX_NUM_RELATIONS_RESULTS: usize = 500;

pub struct RelationsReaderService {
    pub index: Index,
    pub schema: Schema,
    pub reader: IndexReader,
}

impl Debug for RelationsReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldReaderService").field("index", &self.index).field("schema", &self.schema).finish()
    }
}

impl RelationsReaderService {
    pub fn search(&self, request: &RelationSearchRequest) -> anyhow::Result<RelationSearchResponse> {
        Ok(RelationSearchResponse {
            subgraph: self.graph_search(request)?,
            prefix: self.prefix_search(request)?,
        })
    }

    pub fn advanced_graph_query(&self, query: GraphQuery) -> anyhow::Result<Vec<nidx_protos::Relation>> {
        let searcher = GraphSearcher::new(self.reader.searcher());
        searcher.search(query)
    }
}

impl RelationsReaderService {
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Invalid path {:?}", path));
        }
        let field_schema = Schema::new();
        let index = Index::open_in_dir(path)?;

        let reader = index.reader_builder().try_into()?;

        Ok(RelationsReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    fn graph_search(&self, request: &RelationSearchRequest) -> anyhow::Result<Option<EntitiesSubgraphResponse>> {
        let Some(bfs_request) = request.subgraph.as_ref() else {
            return Ok(None);
        };

        if bfs_request.depth() != 1 {
            return Err(anyhow::anyhow!("Depth must be 1 right now"));
        }

        if bfs_request.entry_points.is_empty() {
            return Ok(Some(EntitiesSubgraphResponse::default()));
        }

        let mut statements = vec![];

        // Entry points are source or target nodes we want to search for. We want any undirected
        // path containing any entry point
        statements.push(Expression::Or(
            bfs_request
                .entry_points
                .iter()
                .map(|entry_point| {
                    PathQuery::UndirectedPath((
                        Expression::Value(Node {
                            value: Some(entry_point.value.clone().into()),
                            node_type: Some(entry_point.ntype()),
                            node_subtype: Some(entry_point.subtype.clone()),
                        }),
                        Expression::Value(Relation::default()),
                        Expression::Value(Node::default()),
                    ))
                })
                .collect(),
        ));

        // A query can specifiy nodes marked as deleted in the db (but not removed from the index).
        // We want to exclude any path containing any of those nodes.
        //
        // The request groups values per subtype (to optimize request size) but, as we don't support
        // OR at node value level, we'll split them.
        for deleted_nodes in bfs_request.deleted_entities.iter() {
            if deleted_nodes.node_values.is_empty() {
                continue;
            }

            statements.push(Expression::Not(PathQuery::UndirectedPath((
                Expression::Or(
                    deleted_nodes
                        .node_values
                        .iter()
                        .map(|deleted_entity_value| Node {
                            value: Some(deleted_entity_value.clone().into()),
                            node_subtype: Some(deleted_nodes.node_subtype.clone()),
                            ..Default::default()
                        })
                        .collect(),
                ),
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            ))));
        }

        // Subtypes can also be marked as deleted in the db (but kept in the index). We also want to
        // exclude any triplet containg a node with such subtypes
        let excluded_subtypes: Vec<_> = bfs_request
            .deleted_groups
            .iter()
            .map(|deleted_subtype| Node {
                node_subtype: Some(deleted_subtype.clone()),
                ..Default::default()
            })
            .collect();
        if excluded_subtypes.len() > 0 {
            statements.push(Expression::Not(PathQuery::UndirectedPath((
                Expression::Or(excluded_subtypes),
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            ))));
        }

        let query_parser = GraphQueryParser::new();
        let query = query_parser.parse_graph_query(GraphQuery::MultiStatement(statements));
        let searcher = self.reader.searcher();

        let topdocs = TopDocs::with_limit(MAX_NUM_RELATIONS_RESULTS);
        let matching_docs = searcher.search(&query, &topdocs)?;
        let mut response = EntitiesSubgraphResponse::default();

        for (_, doc_addr) in matching_docs {
            let source = searcher.doc(doc_addr)?;
            let relation = io_maps::doc_to_relation(&self.schema, &source);
            response.relations.push(relation);
        }

        Ok(Some(response))
    }

    fn prefix_search(&self, request: &RelationSearchRequest) -> anyhow::Result<Option<RelationPrefixSearchResponse>> {
        let Some(prefix_request) = request.prefix.as_ref() else {
            return Ok(None);
        };

        let Some(search) = &prefix_request.search else {
            return Err(anyhow::anyhow!("Search terms needed"));
        };

        let query_parser = GraphQueryParser::new();
        let mut source_statements = vec![];
        let mut target_statements = vec![];

        // Filter only nodes with with specified types/subtypes
        for node_filter in prefix_request.node_filters.iter() {
            source_statements.push(Expression::Value(PathQuery::DirectedPath((
                Expression::Value(Node {
                    node_type: Some(node_filter.node_type()),
                    node_subtype: node_filter.node_subtype.clone(),
                    ..Default::default()
                }),
                Expression::Value(Relation::default()),
                Expression::Value(Node::default()),
            ))));

            target_statements.push(Expression::Value(PathQuery::DirectedPath((
                Expression::Value(Node::default()),
                Expression::Value(Relation::default()),
                Expression::Value(Node {
                    node_type: Some(node_filter.node_type()),
                    node_subtype: node_filter.node_subtype.clone(),
                    ..Default::default()
                }),
            ))));
        }

        let mut source_q: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        let mut target_q: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        let searcher = self.reader.searcher();
        let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
        let schema = &self.schema;

        match search {
            Search::Query(query) => {
                // This search is intended to do a normal tokenized search on entities names. However, since we
                // do some custom normalization for these fields, we need to do some custom handling here.
                // Feel free to replace this with something better if we start indexing entities name with tokenization.
                let mut source_prefix_q = Vec::new();
                let mut target_prefix_q = Vec::new();
                // Search for all groups of words in the query, e.g:
                // query "Films with James Bond"
                // returns:
                // "Films", "with", "James", "Bond"
                // "Films with", "with James", "James Bond"
                // "Films with James", "with James Bond"
                let words: Vec<_> = query.split_whitespace().collect();
                for end in 1..=words.len() {
                    for len in 1..=ENTITY_WORD_SIZE {
                        if len > end {
                            break;
                        }
                        let start = end - len;
                        self.add_fuzzy_prefix_query(&mut source_prefix_q, &mut target_prefix_q, &words[start..end]);
                    }
                }
                source_q.push((Occur::Must, Box::new(BooleanQuery::new(source_prefix_q))));
                target_q.push((Occur::Must, Box::new(BooleanQuery::new(target_prefix_q))));
            }
            Search::Prefix(prefix) => {
                let normalized_prefix = schema::normalize(prefix);
                source_q.push((
                    Occur::Must,
                    Box::new(FuzzyTermQuery::new_prefix(
                        Term::from_field_text(self.schema.normalized_source_value, &normalized_prefix),
                        FUZZY_DISTANCE,
                        true,
                    )),
                ));
                target_q.push((
                    Occur::Must,
                    Box::new(FuzzyTermQuery::new_prefix(
                        Term::from_field_text(self.schema.normalized_target_value, &normalized_prefix),
                        FUZZY_DISTANCE,
                        true,
                    )),
                ));
            }
        }

        if !source_statements.is_empty() {
            source_q.push((Occur::Must, query_parser.parse_graph_query(GraphQuery::MultiStatement(source_statements))));
        };

        if !target_statements.is_empty() {
            target_q.push((Occur::Must, query_parser.parse_graph_query(GraphQuery::MultiStatement(target_statements))));
        };

        let source_prefix_query = BooleanQuery::new(source_q);
        let target_prefix_query = BooleanQuery::new(target_q);

        let mut response = RelationPrefixSearchResponse::default();
        let mut results = HashSet::new();
        for (_, source_res_addr) in searcher.search(&source_prefix_query, &topdocs)? {
            let source_res_doc = searcher.doc(source_res_addr)?;
            let relation_node = io_maps::source_to_relation_node(schema, &source_res_doc);
            results.insert(HashedRelationNode(relation_node));
        }
        for (_, source_res_addr) in searcher.search(&target_prefix_query, &topdocs)? {
            let source_res_doc = searcher.doc(source_res_addr)?;
            let relation_node = io_maps::target_to_relation_node(schema, &source_res_doc);
            results.insert(HashedRelationNode(relation_node));
        }
        response.nodes = results.into_iter().map(Into::into).collect();
        Ok(Some(response))
    }

    fn add_fuzzy_prefix_query(
        &self,
        source_queries: &mut Vec<(Occur, Box<dyn Query>)>,
        target_queries: &mut Vec<(Occur, Box<dyn Query>)>,
        prefix: &[&str],
    ) {
        let normalized_prefix = schema::normalize_words(prefix.iter().copied());
        source_queries.push((
            Occur::Should,
            Box::new(FuzzyTermQuery::new(
                Term::from_field_text(self.schema.normalized_source_value, &normalized_prefix),
                FUZZY_DISTANCE,
                true,
            )),
        ));
        target_queries.push((
            Occur::Should,
            Box::new(FuzzyTermQuery::new(
                Term::from_field_text(self.schema.normalized_target_value, &normalized_prefix),
                FUZZY_DISTANCE,
                true,
            )),
        ));
    }
}

pub struct HashedRelationNode(pub RelationNode);

impl From<HashedRelationNode> for RelationNode {
    fn from(val: HashedRelationNode) -> Self {
        val.0
    }
}

impl From<RelationNode> for HashedRelationNode {
    fn from(val: RelationNode) -> Self {
        HashedRelationNode(val)
    }
}

impl Eq for HashedRelationNode {}
impl PartialEq for HashedRelationNode {
    fn eq(&self, other: &Self) -> bool {
        let inner = &self.0;
        let other_inner = &other.0;
        let lhs = (&inner.value, &inner.ntype, &inner.subtype);
        let rhs = (&other_inner.value, &other_inner.ntype, &other_inner.subtype);
        lhs.eq(&rhs)
    }
}
impl std::hash::Hash for HashedRelationNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let inner = &self.0;
        (&inner.value, &inner.ntype, &inner.subtype).hash(state)
    }
}

#[cfg(test)]
mod tests {
    // This is the outer module
    use super::*;

    #[test]
    fn test_hash_multiple_values() -> anyhow::Result<()> {
        let mut results = HashSet::new();
        results.insert(HashedRelationNode(RelationNode {
            value: "value".to_string(),
            ntype: 1_i32,
            subtype: "subtype".to_string(),
        }));
        results.insert(HashedRelationNode(RelationNode {
            value: "value".to_string(),
            ntype: 1_i32,
            subtype: "subtype".to_string(),
        }));

        assert_eq!(results.len(), 1);

        Ok(())
    }
}
