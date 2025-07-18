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
use std::fmt::Debug;
use std::path::Path;

use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::{GraphSearchRequest, GraphSearchResponse, RelationNode};
use nidx_types::prefilter::{FieldId, PrefilterResult};
use tantivy::collector::TopDocs;
use tantivy::query::{BooleanQuery, EmptyQuery, Query, TermSetQuery};
use tantivy::schema::Field;
use tantivy::{Index, IndexReader};
use uuid::Uuid;

use crate::graph_collector::{Selector, TopUniqueCollector};
use crate::graph_query_parser::{BoolGraphQuery, BoolNodeQuery, GraphQueryParser};
use crate::schema::{Schema, decode_node, decode_relation, encode_field_id};
use crate::top_unique_n::TopUniqueN;
use crate::{RelationConfig, io_maps};

pub const FUZZY_DISTANCE: u8 = 1;
pub const NUMBER_OF_RESULTS_SUGGEST: usize = 20;

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

        let term = tantivy::Term::from_field_bytes(self.field, &encode_field_id(next.resource_id, &next.field_id[1..]));
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
                metadata: io_maps::decode_metadata(&self.schema, &doc),
                resource_field_id: io_maps::doc_to_resource_field_id(&self.schema, &doc),
                facets: io_maps::doc_to_facets(&self.schema, &doc),
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
    use std::collections::HashSet;

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
