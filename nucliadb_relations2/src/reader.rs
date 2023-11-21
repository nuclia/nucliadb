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

use nucliadb_core::prelude::*;
use nucliadb_core::protos::{
    EdgeList, EntitiesSubgraphResponse, RelationEdge, RelationNode, RelationPrefixSearchResponse,
    RelationSearchRequest, RelationSearchResponse, RelationTypeListMember, TypeList,
};
use nucliadb_core::tracing::{self, *};
use nucliadb_procs::measure;
use tantivy::collector::{Count, DocSetCollector, TopDocs};
use tantivy::query::{AllQuery, BooleanQuery, FuzzyTermQuery, Occur, Query, TermQuery};
use tantivy::schema::IndexRecordOption;
use tantivy::{Index, IndexReader, ReloadPolicy, Term};

use crate::schema::Schema;
use crate::{io_maps, schema};

const FUZZY_DISTANCE: u8 = 1;
const NUMBER_OF_RESULTS_SUGGEST: usize = 10;

pub struct RelationsReaderService {
    index: Index,
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

impl RelationReader for RelationsReaderService {
    #[measure(actor = "relations", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let searcher = self.reader.searcher();
        let total = searcher.search(&AllQuery, &Count)?;
        Ok(total)
    }

    #[measure(actor = "relations", metric = "get_edges")]
    #[tracing::instrument(skip_all)]
    fn get_edges(&self) -> NodeResult<EdgeList> {
        let schema = &self.schema;
        let searcher = self.reader.searcher();
        let all_docs = searcher.search(&AllQuery, &DocSetCollector)?;
        let mut collector = HashSet::new();

        for doc_address in all_docs {
            let doc = searcher.doc(doc_address)?;

            let relation_type: i32 = io_maps::u64_to_relation_type(schema.relationship(&doc));
            let relation_label = schema.relationship_label(&doc);

            let relation = (relation_type, relation_label);
            collector.insert(relation);
        }

        let list = collector
            .into_iter()
            .map(|(edge_type, property)| RelationEdge {
                property,
                edge_type,
            })
            .collect();
        Ok(EdgeList { list })
    }

    #[measure(actor = "relations", metric = "get_node_types")]
    #[tracing::instrument(skip_all)]
    fn get_node_types(&self) -> NodeResult<TypeList> {
        let schema = &self.schema;
        let searcher = self.reader.searcher();
        let all_docs = searcher.search(&AllQuery, &DocSetCollector)?;
        let mut collector = HashSet::new();

        for doc_address in all_docs {
            let doc = searcher.doc(doc_address)?;

            let source_type: i32 = io_maps::u64_to_node_type(schema.source_type(&doc));
            let source_subtype = schema.source_subtype(&doc);
            let source = (source_type, source_subtype);
            collector.insert(source);

            let target_type: i32 = io_maps::u64_to_node_type(schema.target_type(&doc));
            let target_subtype = schema.target_subtype(&doc);
            let target = (target_type, target_subtype);
            collector.insert(target);
        }
        let list: Vec<_> = collector
            .into_iter()
            .map(|raw_type| RelationTypeListMember {
                with_type: raw_type.0,
                with_subtype: raw_type.1,
            })
            .collect();
        Ok(TypeList { list })
    }
}

impl ReaderChild for RelationsReaderService {
    type Request = RelationSearchRequest;
    type Response = RelationSearchResponse;

    #[measure(actor = "relations", metric = "search")]
    #[tracing::instrument(skip_all)]
    fn search(&self, request: &Self::Request) -> NodeResult<Self::Response> {
        Ok(RelationSearchResponse {
            subgraph: self.graph_search(request)?,
            prefix: self.prefix_search(request)?,
        })
    }

    #[measure(actor = "relations", metric = "stored_ids")]
    #[tracing::instrument(skip_all)]
    fn stored_ids(&self) -> NodeResult<Vec<String>> {
        let mut node_values = HashSet::new();
        let searcher = self.reader.searcher();
        for addr in searcher.search(&AllQuery, &DocSetCollector)? {
            let document = searcher.doc(addr)?;
            let source_value = self.schema.source_value(&document);
            let target_value = self.schema.target_value(&document);
            node_values.insert(source_value);
            node_values.insert(target_value);
        }
        let node_values: Vec<_> = node_values.into_iter().collect();
        Ok(node_values)
    }
}

impl RelationsReaderService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &RelationConfig) -> NodeResult<Self> {
        if !config.path.exists() {
            return Err(node_error!("Invalid path {:?}", config.path));
        }
        let field_schema = Schema::new();
        let index = Index::open_in_dir(&config.path)?;

        let reader = index
            .reader_builder()
            .reload_policy(ReloadPolicy::OnCommit)
            .try_into()?;

        Ok(RelationsReaderService {
            index,
            reader,
            schema: field_schema,
        })
    }

    #[tracing::instrument(skip_all)]
    fn graph_search(
        &self,
        request: &RelationSearchRequest,
    ) -> NodeResult<Option<EntitiesSubgraphResponse>> {
        let Some(bfs_request) = request.subgraph.as_ref() else {
            return Ok(None);
        };

        if bfs_request.depth() != 1 {
            return Err(node_error!("Depth must be 1 right now"));
        }

        if bfs_request.entry_points.is_empty() {
            return Ok(Some(EntitiesSubgraphResponse::default()));
        }

        let mut queries = Vec::new();

        for node in bfs_request.entry_points.iter() {
            let normalized_value = schema::normalize(&node.value);
            let node_subtype = &node.subtype;
            let node_type = io_maps::node_type_to_u64(node.ntype());

            // Out relations
            let source_value_term = TermQuery::new(
                Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                IndexRecordOption::Basic,
            );
            let source_type_term = TermQuery::new(
                Term::from_field_u64(self.schema.source_type, node_type),
                IndexRecordOption::Basic,
            );
            let source_subtype_term = TermQuery::new(
                Term::from_field_text(self.schema.source_subtype, node_subtype),
                IndexRecordOption::Basic,
            );
            let out_relations_query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(source_value_term)),
                (Occur::Must, Box::new(source_type_term)),
                (Occur::Must, Box::new(source_subtype_term)),
            ]));

            // In relations
            let target_value_term = TermQuery::new(
                Term::from_field_text(self.schema.normalized_target_value, &normalized_value),
                IndexRecordOption::Basic,
            );
            let target_type_term = TermQuery::new(
                Term::from_field_u64(self.schema.target_type, node_type),
                IndexRecordOption::Basic,
            );
            let target_subtype_term = TermQuery::new(
                Term::from_field_text(self.schema.target_subtype, node_subtype),
                IndexRecordOption::Basic,
            );
            let in_relations_query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Must, Box::new(target_value_term)),
                (Occur::Must, Box::new(target_type_term)),
                (Occur::Must, Box::new(target_subtype_term)),
            ]));

            queries.push((Occur::Should, out_relations_query));
            queries.push((Occur::Should, in_relations_query));
        }

        // filter out deletions
        for deleted_nodes in bfs_request.deleted_entities.iter() {
            if deleted_nodes.node_values.is_empty() {
                continue;
            }

            let source_subtype_filter: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.schema.source_subtype, &deleted_nodes.node_subtype),
                IndexRecordOption::Basic,
            ));
            let target_subtype_filter: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.schema.target_subtype, &deleted_nodes.node_subtype),
                IndexRecordOption::Basic,
            ));

            let mut source_value_subqueries = Vec::new();
            let mut target_value_subqueries = Vec::new();
            for deleted_entity_value in deleted_nodes.node_values.iter() {
                let normalized_value = schema::normalize(deleted_entity_value);
                let exclude_source_value: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(self.schema.normalized_source_value, &normalized_value),
                    IndexRecordOption::Basic,
                ));
                let exclude_target_value: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(self.schema.normalized_target_value, &normalized_value),
                    IndexRecordOption::Basic,
                ));
                source_value_subqueries.push((Occur::Should, exclude_source_value));
                target_value_subqueries.push((Occur::Should, exclude_target_value));
            }

            let source_value_filter: Box<dyn Query> =
                Box::new(BooleanQuery::new(source_value_subqueries));
            let source_exclusion_query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Must, source_subtype_filter),
                (Occur::Must, source_value_filter),
            ]));
            queries.push((Occur::MustNot, source_exclusion_query));

            let target_value_filter: Box<dyn Query> =
                Box::new(BooleanQuery::new(target_value_subqueries));
            let target_exclusion_query: Box<dyn Query> = Box::new(BooleanQuery::new(vec![
                (Occur::Must, target_subtype_filter),
                (Occur::Must, target_value_filter),
            ]));
            queries.push((Occur::MustNot, target_exclusion_query));
        }

        let mut excluded_subtype_queries = Vec::new();
        for deleted_subtype in bfs_request.deleted_groups.iter() {
            let exclude_from_source: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.schema.source_subtype, deleted_subtype),
                IndexRecordOption::Basic,
            ));
            let exclude_from_target: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_text(self.schema.target_subtype, deleted_subtype),
                IndexRecordOption::Basic,
            ));
            excluded_subtype_queries.push((Occur::Should, exclude_from_source));
            excluded_subtype_queries.push((Occur::Should, exclude_from_target));
        }
        let excluded_subtypes: Box<dyn Query> =
            Box::new(BooleanQuery::new(excluded_subtype_queries));
        queries.push((Occur::MustNot, excluded_subtypes));

        let query = BooleanQuery::from(queries);
        let searcher = self.reader.searcher();
        let matching_docs = searcher.search(&query, &DocSetCollector)?;
        let mut response = EntitiesSubgraphResponse::default();

        for doc_addr in matching_docs {
            let source = searcher.doc(doc_addr)?;
            let relation = io_maps::doc_to_relation(&self.schema, &source);
            response.relations.push(relation);
        }

        Ok(Some(response))
    }

    #[tracing::instrument(skip_all)]
    fn prefix_search(
        &self,
        request: &RelationSearchRequest,
    ) -> NodeResult<Option<RelationPrefixSearchResponse>> {
        let Some(prefix_request) = request.prefix.as_ref() else {
            return Ok(None);
        };

        // if prefix_request.prefix.is_empty() {
        //     return Ok(Some(RelationPrefixSearchResponse::default()));
        // }

        let prefix = schema::normalize(&prefix_request.prefix);
        let searcher = self.reader.searcher();
        let topdocs = TopDocs::with_limit(NUMBER_OF_RESULTS_SUGGEST);
        let schema = &self.schema;

        let mut source_types = vec![];
        let mut target_types = vec![];

        for node_filter in prefix_request.node_filters.iter() {
            let mut source_clause = vec![];
            let mut target_clause = vec![];
            let node_subtype = node_filter.node_subtype();
            let node_type = io_maps::node_type_to_u64(node_filter.node_type());

            let source_type_query: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_u64(schema.source_type, node_type),
                IndexRecordOption::Basic,
            ));
            let target_type_query: Box<dyn Query> = Box::new(TermQuery::new(
                Term::from_field_u64(schema.target_type, node_type),
                IndexRecordOption::Basic,
            ));
            source_clause.push((Occur::Must, source_type_query));
            target_clause.push((Occur::Must, target_type_query));

            if !node_subtype.is_empty() {
                let subtype_source: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(schema.source_subtype, node_subtype),
                    IndexRecordOption::Basic,
                ));
                let subtype_target: Box<dyn Query> = Box::new(TermQuery::new(
                    Term::from_field_text(schema.target_subtype, node_subtype),
                    IndexRecordOption::Basic,
                ));
                source_clause.push((Occur::Must, subtype_source));
                target_clause.push((Occur::Must, subtype_target));
            }

            let source_clause: Box<dyn Query> = Box::new(BooleanQuery::new(source_clause));
            let target_clause: Box<dyn Query> = Box::new(BooleanQuery::new(target_clause));
            source_types.push((Occur::Should, source_clause));
            target_types.push((Occur::Should, target_clause));
        }

        let source_typing_query: Box<dyn Query> = if source_types.is_empty() {
            Box::new(AllQuery)
        } else {
            Box::new(BooleanQuery::new(source_types))
        };

        let target_typing_query: Box<dyn Query> = if target_types.is_empty() {
            Box::new(AllQuery)
        } else {
            Box::new(BooleanQuery::new(target_types))
        };

        let source_value_query: Box<dyn Query> = Box::new(FuzzyTermQuery::new_prefix(
            Term::from_field_text(schema.normalized_source_value, &prefix),
            FUZZY_DISTANCE,
            true,
        ));
        let target_value_query: Box<dyn Query> = Box::new(FuzzyTermQuery::new_prefix(
            Term::from_field_text(schema.normalized_target_value, &prefix),
            FUZZY_DISTANCE,
            true,
        ));

        let source_prefix_query = BooleanQuery::new(vec![
            (Occur::Must, source_value_query),
            (Occur::Must, source_typing_query),
        ]);
        let target_prefix_query = BooleanQuery::new(vec![
            (Occur::Must, target_value_query),
            (Occur::Must, target_typing_query),
        ]);

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
}

struct HashedRelationNode(RelationNode);

impl From<HashedRelationNode> for RelationNode {
    fn from(val: HashedRelationNode) -> Self {
        val.0
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
    fn test_hash_multiple_values() -> NodeResult<()> {
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
