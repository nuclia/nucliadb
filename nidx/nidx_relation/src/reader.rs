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
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, Occur, Query};
use tantivy::{DocAddress, Index, IndexReader, Searcher};

use crate::graph_collector::{NodeSelector, TopUniqueNodeCollector, TopUniqueRelationCollector};
use crate::graph_query_parser::{
    BoolGraphQuery, BoolNodeQuery, Expression, FuzzyTerm, GraphQuery, GraphQueryParser, Node, NodeQuery, Term,
};
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
        f.debug_struct("FieldReaderService")
            .field("index", &self.index)
            .field("schema", &self.schema)
            .finish()
    }
}

impl RelationsReaderService {
    pub fn relation_search(&self, request: &RelationSearchRequest) -> anyhow::Result<RelationSearchResponse> {
        Ok(RelationSearchResponse {
            subgraph: self.entities_subgraph_search(request)?,
            prefix: self.prefix_search(request)?,
        })
    }

    pub fn graph_search(&self, request: &GraphSearchRequest) -> anyhow::Result<GraphSearchResponse> {
        // No query? Empty graph
        let Some(query) = &request.query else {
            return Ok(GraphSearchResponse::default());
        };
        let Some(query) = &query.path else {
            return Ok(GraphSearchResponse::default());
        };

        let top_k = request.top_k as usize;

        let response = match request.kind() {
            QueryKind::Path => self.paths_graph_search(query, top_k),
            QueryKind::Nodes => self.nodes_graph_search(query, top_k),
            QueryKind::Relations => self.relations_graph_search(query, top_k),
        };
        response
    }

    pub fn inner_graph_search(&self, query: GraphQuery) -> anyhow::Result<nidx_protos::GraphSearchResponse> {
        let parser = GraphQueryParser::new();
        let index_query: Box<dyn Query> = parser.parse(query);

        // TODO: parametrize this magic constant
        let collector = TopDocs::with_limit(1000);

        let searcher = self.reader.searcher();
        let matching_docs = searcher.search(&index_query, &collector)?;

        self.build_graph_response(
            &searcher,
            matching_docs.into_iter().map(|(_score, doc_address)| doc_address),
        )
    }

    fn paths_graph_search(
        &self,
        query: &nidx_protos::graph_query::PathQuery,
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolGraphQuery::try_from(query)?;
        let parser = GraphQueryParser::new();
        let index_query = parser.parse_bool(query);

        let collector = TopDocs::with_limit(top_k);
        let searcher = self.reader.searcher();
        let matching_docs = searcher.search(&index_query, &collector)?;
        self.build_graph_response(
            &searcher,
            matching_docs.into_iter().map(|(_score, doc_address)| doc_address),
        )
    }

    fn nodes_graph_search(
        &self,
        query: &nidx_protos::graph_query::PathQuery,
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolNodeQuery::try_from(query)?;
        let parser = GraphQueryParser::new();
        let (source_query, destination_query) = parser.parse_bool_node(query);

        let mut unique_nodes = HashSet::new();

        let collector = TopUniqueNodeCollector::new(NodeSelector::SourceNodes, top_k);
        let searcher = self.reader.searcher();
        let mut source_nodes = searcher.search(&source_query, &collector)?;
        unique_nodes.extend(source_nodes.drain());

        let collector = TopUniqueNodeCollector::new(NodeSelector::DestinationNodes, top_k);
        let searcher = self.reader.searcher();
        let mut destination_nodes = searcher.search(&destination_query, &collector)?;
        unique_nodes.extend(destination_nodes.drain());

        let nodes = unique_nodes
            .into_iter()
            .map(|(value, node_type, node_subtype)| RelationNode {
                value,
                ntype: node_type,
                subtype: node_subtype,
            })
            .take(top_k)
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
        top_k: usize,
    ) -> anyhow::Result<GraphSearchResponse> {
        let query = BoolGraphQuery::try_from(query)?;
        let parser = GraphQueryParser::new();
        let index_query = parser.parse_bool(query);

        let collector = TopUniqueRelationCollector::new(top_k);
        let searcher = self.reader.searcher();
        let matching_docs = searcher.search(&index_query, &collector)?;

        let relations = matching_docs
            .into_iter()
            .map(|doc| io_maps::doc_to_graph_relation(&self.schema, &doc))
            .collect();

        let response = nidx_protos::GraphSearchResponse {
            relations,
            ..Default::default()
        };
        Ok(response)
    }

    fn build_graph_response(
        &self,
        searcher: &Searcher,
        docs: impl Iterator<Item = DocAddress>,
    ) -> anyhow::Result<nidx_protos::GraphSearchResponse> {
        // We are being very naive and writing everything to the proto response. We could be smarter
        // and deduplicates nodes and relations. As paths are pointers, this would improve proto
        // size and ser/de time at expenses of deduplication effort.

        let mut nodes = Vec::new();
        let mut relations = Vec::new();
        let mut graph = Vec::new();

        for doc_address in docs {
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

        let query_parser = GraphQueryParser::new();
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

        if excluded_subtypes.len() > 0 {
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
            Search::Query(query) => {
                // This search is intended to do a normal tokenized search on entities names. However, since we
                // do some custom normalization for these fields, we need to do some custom handling here.
                // Feel free to replace this with something better if we start indexing entities name with tokenization.
                let mut prefix_nodes_q = Vec::new();

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
                        let prefix = &words[start..end];

                        let normalized_prefix = schema::normalize_words(prefix.iter().copied());
                        prefix_nodes_q.push(Node {
                            value: Some(Term::Fuzzy(FuzzyTerm {
                                value: normalized_prefix,
                                fuzzy_distance: FUZZY_DISTANCE,
                                // BUG: this should be true if we want prefix search
                                is_prefix: false,
                            })),
                            ..Default::default()
                        });
                    }
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
                let normalized_prefix = schema::normalize(prefix);
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
