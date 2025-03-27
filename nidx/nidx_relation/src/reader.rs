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

use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::relation_prefix_search_request::Search;
use nidx_protos::{
    EntitiesSubgraphResponse, GraphSearchRequest, GraphSearchResponse, RelationNode, RelationPrefixSearchResponse,
    RelationSearchRequest, RelationSearchResponse,
};
use nidx_types::prefilter::{FieldId, PrefilterResult};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, EmptyQuery, Occur, Query, TermSetQuery};
use tantivy::schema::Field;
use tantivy::{Index, IndexReader};
use uuid::Uuid;

use crate::graph_collector::{Selector, TopUniqueCollector};
use crate::graph_query_parser::{
    BoolGraphQuery, BoolNodeQuery, Expression, FuzzyTerm, GraphQuery, GraphQueryParser, Node, NodeQuery, Term,
};
use crate::schema::{Schema, decode_node, decode_relation, encode_field_id};
use crate::top_unique_n::TopUniqueN;
use crate::{RelationConfig, io_maps};

pub const FUZZY_DISTANCE: u8 = 1;
pub const NUMBER_OF_RESULTS_SUGGEST: usize = 20;
// Hard limit until we have pagination in place
const MAX_NUM_RELATIONS_RESULTS: usize = 500;

pub struct RelationsReaderService {
    pub index: Index,
    pub schema: Schema,
    pub reader: IndexReader,
}

impl Debug for RelationsReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FieldReaderService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

struct AddMetadataFieldIterator<'a, I: Iterator<Item = &'a FieldId>> {
    buffer: Option<tantivy::Term>,
    prev: Uuid,
    field: Field,
    iter: I,
}

impl<'a, I: Iterator<Item = &'a FieldId>> AddMetadataFieldIterator<'a, I> {
    fn new(field: Field, iter: I) -> Self {
        Self {
            buffer: None,
            prev: Uuid::nil(),
            field,
            iter,
        }
    }
}

impl<'a, I: Iterator<Item = &'a FieldId>> Iterator for AddMetadataFieldIterator<'a, I> {
    type Item = tantivy::Term;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(buf) = self.buffer.take() {
            return Some(buf);
        }
        let next = self.iter.next()?;

        let term = tantivy::Term::from_field_bytes(self.field, &encode_field_id(next.resource_id, &next.field_id));
        if self.prev != next.resource_id {
            // New resource, emit the metadata field (and save the actual field for next iteration)
            self.buffer = Some(term);
            Some(tantivy::Term::from_field_bytes(
                self.field,
                &encode_field_id(next.resource_id, "a/metadata"),
            ))
        } else {
            Some(term)
        }
    }
}

impl RelationsReaderService {
    pub fn relation_search(&self, request: &RelationSearchRequest) -> anyhow::Result<RelationSearchResponse> {
        Ok(RelationSearchResponse {
            subgraph: self.entities_subgraph_search(request)?,
            prefix: self.prefix_search(request)?,
        })
    }

    pub fn graph_search(
        &self,
        request: &GraphSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<GraphSearchResponse> {
        // No query? Empty graph
        let Some(query) = &request.query else {
            return Ok(GraphSearchResponse::default());
        };
        let Some(query) = &query.path else {
            return Ok(GraphSearchResponse::default());
        };

        let top_k = request.top_k as usize;

        match request.kind() {
            QueryKind::Path => self.paths_graph_search(query, prefilter, top_k),
            QueryKind::Nodes => self.nodes_graph_search(query, prefilter, top_k),
            QueryKind::Relations => self.relations_graph_search(query, prefilter, top_k),
        }
    }

    fn paths_graph_search(
        &self,
        query: &nidx_protos::graph_query::PathQuery,
        prefilter: &PrefilterResult,
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolGraphQuery::try_from(query)?;
        let parser = GraphQueryParser::new(&self.schema);
        let index_query = parser.parse_bool(query);
        let index_query = self.apply_prefilter(index_query, prefilter);

        let collector = TopDocs::with_limit(top_k);
        let searcher = self.reader.searcher();
        let matching_docs = searcher.search(&index_query, &collector)?;

        let mut nodes = Vec::new();
        let mut relations = Vec::new();
        let mut graph = Vec::new();

        for (_score, doc_address) in matching_docs {
            let doc = searcher.doc(doc_address)?;

            let source = io_maps::source_to_relation_node(&self.schema, &doc);
            let relation = io_maps::doc_to_graph_relation(&self.schema, &doc);
            let destination = io_maps::target_to_relation_node(&self.schema, &doc);

            let source_idx = nodes.len();
            nodes.push(source);
            let relation_idx = relations.len();
            relations.push(relation);
            let destination_idx = nodes.len();
            nodes.push(destination);

            graph.push(nidx_protos::graph_search_response::Path {
                source: source_idx as u32,
                relation: relation_idx as u32,
                destination: destination_idx as u32,
            })
        }

        let response = nidx_protos::GraphSearchResponse {
            nodes,
            relations,
            graph,
        };
        Ok(response)
    }

    fn nodes_graph_search(
        &self,
        query: &nidx_protos::graph_query::PathQuery,
        prefilter: &PrefilterResult,
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolNodeQuery::try_from(query)?;
        let parser = GraphQueryParser::new(&self.schema);
        let (source_query, destination_query) = parser.parse_bool_node(query);
        let source_query = self.apply_prefilter(source_query, prefilter);
        let destination_query = self.apply_prefilter(destination_query, prefilter);

        let searcher = self.reader.searcher();

        let mut unique_nodes = TopUniqueN::new(top_k);

        let collector = TopUniqueCollector::new(Selector::SourceNodes, top_k);
        let source_nodes = searcher.search(&source_query, &collector)?;
        unique_nodes.merge(source_nodes);

        let collector = TopUniqueCollector::new(Selector::DestinationNodes, top_k);
        let destination_nodes = searcher.search(&destination_query, &collector)?;
        unique_nodes.merge(destination_nodes);

        let nodes = unique_nodes
            .into_sorted_vec()
            .into_iter()
            .map(|(encoded_node, _score)| {
                let (value, node_type, node_subtype) = decode_node(&encoded_node);
                RelationNode {
                    value,
                    ntype: io_maps::u64_to_node_type(node_type),
                    subtype: node_subtype,
                }
            })
            .collect();

        let response = nidx_protos::GraphSearchResponse {
            nodes,
            ..Default::default()
        };
        Ok(response)
    }

    fn relations_graph_search(
        &self,
        query: &nidx_protos::graph_query::PathQuery,
        prefilter: &PrefilterResult,
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolGraphQuery::try_from(query)?;
        let parser = GraphQueryParser::new(&self.schema);
        let index_query = parser.parse_bool(query);
        let index_query = self.apply_prefilter(index_query, prefilter);

        let searcher = self.reader.searcher();

        let collector = TopUniqueCollector::new(Selector::Relations, top_k);
        let top_relations = searcher.search(&index_query, &collector)?;

        let relations = top_relations
            .into_sorted_vec()
            .into_iter()
            .map(|(encoded_relation, _score)| {
                let (relation_type, relation_label) = decode_relation(&encoded_relation);
                nidx_protos::graph_search_response::Relation {
                    relation_type: io_maps::u64_to_relation_type::<i32>(relation_type),
                    label: relation_label,
                    metadata: None,
                }
            })
            .collect();

        let response = nidx_protos::GraphSearchResponse {
            relations,
            ..Default::default()
        };
        Ok(response)
    }

    fn apply_prefilter(&self, query: Box<dyn Query>, prefilter: &PrefilterResult) -> Box<dyn Query> {
        match prefilter {
            PrefilterResult::All => query,
            PrefilterResult::None => Box::new(EmptyQuery),
            PrefilterResult::Some(fields) => {
                let terms = AddMetadataFieldIterator::new(self.schema.resource_field_id, fields.iter());
                let prefilter_query = Box::new(TermSetQuery::new(terms));
                Box::new(BooleanQuery::intersection(vec![prefilter_query, query]))
            }
        }
    }
}

impl RelationsReaderService {
    pub fn open(path: &Path, config: RelationConfig) -> anyhow::Result<Self> {
        if !path.exists() {
            return Err(anyhow::anyhow!("Invalid path {:?}", path));
        }
        let field_schema = Schema::new(config.version);
        let index = Index::open_in_dir(path)?;

        let reader = index.reader_builder().try_into()?;

        Ok(RelationsReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    fn entities_subgraph_search(
        &self,
        request: &RelationSearchRequest,
    ) -> anyhow::Result<Option<EntitiesSubgraphResponse>> {
        let Some(bfs_request) = request.subgraph.as_ref() else {
            return Ok(None);
        };

        if bfs_request.depth() != 1 {
            return Err(anyhow::anyhow!("Depth must be 1 right now"));
        }

        if bfs_request.entry_points.is_empty() {
            return Ok(Some(EntitiesSubgraphResponse::default()));
        }

        let query_parser = GraphQueryParser::new(&self.schema);
        let mut statements = vec![];

        // Entry points are source or target nodes we want to search for. We want any undirected
        // path containing any entry point
        for entry_point in bfs_request.entry_points.iter() {
            statements.push((
                Occur::Should,
                query_parser.parse(GraphQuery::NodeQuery(NodeQuery::Node(Expression::Value(Node {
                    value: Some(entry_point.value.clone().into()),
                    node_type: Some(entry_point.ntype()),
                    node_subtype: Some(entry_point.subtype.clone()),
                })))),
            ));
        }

        // A query can specifiy nodes marked as deleted in the db (but not removed from the index).
        // We want to exclude any path containing any of those nodes.
        //
        // The request groups values per subtype (to optimize request size) but, as we don't support
        // OR at node value level, we'll split them.
        for deleted_nodes in bfs_request.deleted_entities.iter() {
            if deleted_nodes.node_values.is_empty() {
                continue;
            }
            statements.push((
                Occur::MustNot,
                query_parser.parse(GraphQuery::NodeQuery(NodeQuery::Node(Expression::Or(
                    deleted_nodes
                        .node_values
                        .iter()
                        .map(|deleted_entity_value| Node {
                            value: Some(deleted_entity_value.clone().into()),
                            node_subtype: Some(deleted_nodes.node_subtype.clone()),
                            ..Default::default()
                        })
                        .collect(),
                )))),
            ));
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

        if !excluded_subtypes.is_empty() {
            statements.push((
                Occur::MustNot,
                query_parser.parse(GraphQuery::NodeQuery(NodeQuery::Node(Expression::Or(
                    excluded_subtypes,
                )))),
            ))
        }

        let query: Box<dyn Query> = Box::new(BooleanQuery::new(statements));
        let searcher = self.reader.searcher();

        let topdocs = TopDocs::with_limit(MAX_NUM_RELATIONS_RESULTS);
        let matching_docs = searcher.search(&query, &topdocs)?;
        let mut response = EntitiesSubgraphResponse::default();

        for (_, doc_addr) in matching_docs {
            let source = searcher.doc(doc_addr)?;
            let index_relation = io_maps::doc_to_index_relation(&self.schema, &source);
            response.relations.push(index_relation);
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

        let query_parser = GraphQueryParser::new(&self.schema);

        let mut source_q: Vec<(Occur, Box<dyn Query>)> = Vec::new();
        let mut target_q: Vec<(Occur, Box<dyn Query>)> = Vec::new();

        // Node filters: only search nodes with the specified type/subtype
        let node_filters: Vec<Node> = prefix_request
            .node_filters
            .iter()
            .map(|node_filter| Node {
                node_type: Some(node_filter.node_type()),
                node_subtype: node_filter.node_subtype.clone(),
                ..Default::default()
            })
            .collect();

        if !node_filters.is_empty() {
            source_q.push((
                Occur::Must,
                query_parser.parse(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Or(
                    node_filters.clone(),
                )))),
            ));
            target_q.push((
                Occur::Must,
                query_parser.parse(GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Or(
                    node_filters,
                )))),
            ))
        }

        let searcher = self.reader.searcher();
        // FIXME: we are using a topdocs collector to get prefix results from source and target
        // nodes. However, we are deduplicating afterwards, and this means we could end up with less
        // results although results may exist. As a quick fix, we increase here the limit of the
        // collector. The proper solution would be implementing a custom collector for this task
        let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST * 2);
        let schema = &self.schema;

        match search {
            // TODO: no longer used, we can remove it
            Search::Query(query) => {
                let mut prefix_nodes_q = Vec::new();

                let words: Vec<_> = query.split_whitespace().collect();
                for word in words {
                    prefix_nodes_q.push(Node {
                        value: Some(Term::FuzzyWord(FuzzyTerm {
                            value: word.to_string(),
                            fuzzy_distance: FUZZY_DISTANCE,
                            is_prefix: false,
                        })),
                        ..Default::default()
                    });
                }

                // add fuzzy query for all prefixes
                source_q.push((
                    Occur::Must,
                    query_parser.parse(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Or(
                        prefix_nodes_q.clone(),
                    )))),
                ));
                target_q.push((
                    Occur::Must,
                    query_parser.parse(GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Or(
                        prefix_nodes_q,
                    )))),
                ));
            }
            Search::Prefix(prefix) => {
                let normalized_prefix = schema.normalize(prefix);
                let node_filter = Node {
                    value: Some(Term::Fuzzy(FuzzyTerm {
                        value: normalized_prefix.clone(),
                        fuzzy_distance: FUZZY_DISTANCE,
                        is_prefix: true,
                    })),
                    ..Default::default()
                };
                source_q.push((
                    Occur::Must,
                    query_parser.parse(GraphQuery::NodeQuery(NodeQuery::SourceNode(Expression::Value(
                        node_filter.clone(),
                    )))),
                ));
                target_q.push((
                    Occur::Must,
                    query_parser.parse(GraphQuery::NodeQuery(NodeQuery::DestinationNode(Expression::Value(
                        node_filter,
                    )))),
                ));
            }
        }

        let source_prefix_query = BooleanQuery::new(source_q);
        let target_prefix_query = BooleanQuery::new(target_q);

        let mut response = RelationPrefixSearchResponse::default();
        let mut results = HashSet::new();
        for (_, source_res_addr) in searcher.search(&source_prefix_query, &topdocs)? {
            let source_res_doc = searcher.doc(source_res_addr)?;
            let relation_node = io_maps::source_to_relation_node(schema, &source_res_doc);
            results.insert(HashedRelationNode(relation_node));
        }
        for (_, target_res_addr) in searcher.search(&target_prefix_query, &topdocs)? {
            let target_res_doc = searcher.doc(target_res_addr)?;
            let relation_node = io_maps::target_to_relation_node(schema, &target_res_doc);
            results.insert(HashedRelationNode(relation_node));
        }
        response.nodes = results
            .into_iter()
            .take(NUMBER_OF_RESULTS_SUGGEST)
            .map(Into::into)
            .collect();
        Ok(Some(response))
    }
}

#[derive(Debug)]
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
