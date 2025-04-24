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

mod graph_collector;
pub mod graph_query_parser;
mod io_maps;
mod reader;
mod resource_indexer;
mod schema;
mod top_unique_n;

use anyhow::anyhow;
use nidx_protos::graph_query;
use nidx_protos::graph_query::node::FuzzyMatch;
use nidx_protos::graph_query::node::MatchLocation;
use nidx_protos::graph_query::node::NewMatchKind;
use nidx_protos::graph_query::path_query;
use nidx_protos::graph_search_request::QueryKind;
use nidx_protos::{
    GraphSearchRequest, GraphSearchResponse, RelationNode, relation_node::NodeType, resource::ResourceStatus,
};
use nidx_tantivy::{
    TantivyIndexer, TantivyMeta, TantivySegmentMetadata,
    index_reader::{DeletionQueryBuilder, open_index_with_deletions},
};
use nidx_types::{OpenIndexMetadata, prefilter::PrefilterResult};
use reader::{FUZZY_DISTANCE, NUMBER_OF_RESULTS_SUGGEST, RelationsReaderService};
use resource_indexer::index_relations;
pub use schema::Schema as RelationSchema;
use schema::encode_field_id;
use serde::{Deserialize, Serialize};
use std::path::Path;
use tantivy::{
    Term,
    directory::MmapDirectory,
    indexer::merge_indices,
    query::{Query, TermSetQuery},
    schema::Field,
};
use tracing::{error, instrument};
use uuid::Uuid;

/// Minimum length for a word to be accepted as a entity to search for
/// suggestions. Low values can provide too much noise and higher ones can
/// remove important words from suggestion
const MIN_SUGGEST_PREFIX_LENGTH: usize = 2;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RelationConfig {
    #[serde(default = "default_version")]
    pub version: u64,
}

impl Default for RelationConfig {
    fn default() -> Self {
        Self {
            // This is the default version when creating a new index.
            // Should typically be set to the latest supported version
            version: 2,
        }
    }
}

// This is the default version when reading from serde, i.e: no info on database
// This should always be 1
fn default_version() -> u64 {
    1
}

pub struct RelationIndexer;

pub struct RelationDeletionQueryBuilder {
    resource: Field,
    field: Field,
}
impl DeletionQueryBuilder for RelationDeletionQueryBuilder {
    fn query<'a>(&self, keys: impl Iterator<Item = &'a String>) -> Box<dyn Query> {
        Box::new(TermSetQuery::new(keys.filter_map(|k| {
            // Our keys can be resource or field ids, match the corresponding tantivy field
            if k.len() < 32 {
                error!(?k, "Invalid deletion key for nidx_relation");
                return None;
            }

            let Ok(rid) = Uuid::parse_str(&k[..32]) else {
                error!(?k, "Invalid deletion key for nidx_relation");
                return None;
            };

            let is_field = k.len() > 32;
            if is_field {
                Some(Term::from_field_bytes(self.field, &encode_field_id(rid, &k[33..])))
            } else {
                Some(Term::from_field_bytes(self.resource, rid.as_bytes()))
            }
        })))
    }
}
impl RelationDeletionQueryBuilder {
    fn new(schema: &RelationSchema) -> Self {
        RelationDeletionQueryBuilder {
            resource: schema.resource_id,
            field: schema.resource_field_id,
        }
    }
}

impl RelationIndexer {
    #[instrument(name = "relation::index_resource", skip_all)]
    pub fn index_resource(
        &self,
        output_dir: &Path,
        config: &RelationConfig,
        resource: &nidx_protos::Resource,
    ) -> anyhow::Result<Option<TantivySegmentMetadata>> {
        if config.version != 2 {
            return Err(anyhow!("Unsupported nidx_relation version"));
        }
        let field_schema = RelationSchema::new(config.version);
        let mut indexer = TantivyIndexer::new(output_dir.to_path_buf(), field_schema.schema.clone())?;

        if resource.status == ResourceStatus::Delete as i32 {
            return Err(anyhow::anyhow!("This is a deletion, not a set resource"));
        }

        index_relations(&mut indexer, resource, field_schema)?;
        indexer.finalize()
    }

    pub fn deletions_for_resource(&self, _config: &RelationConfig, resource: &nidx_protos::Resource) -> Vec<String> {
        let rid = &resource.resource.as_ref().unwrap().uuid;
        resource
            .relation_fields_to_delete
            .iter()
            .map(|f| format!("{rid}/{f}"))
            .collect()
    }

    #[instrument(name = "relation::merge", skip_all)]
    pub fn merge(
        &self,
        work_dir: &Path,
        config: RelationConfig,
        open_index: impl OpenIndexMetadata<TantivyMeta>,
    ) -> anyhow::Result<TantivySegmentMetadata> {
        if config.version != 2 {
            return Err(anyhow!("Unsupported nidx_relation version"));
        }
        let schema = RelationSchema::new(config.version);
        let deletions_query = RelationDeletionQueryBuilder::new(&schema);
        let index = open_index_with_deletions(schema.schema, open_index, deletions_query)?;

        let output_index = merge_indices(&[index], MmapDirectory::open(work_dir)?)?;
        let segment = &output_index.searchable_segment_metas()?[0];

        Ok(TantivySegmentMetadata {
            path: work_dir.to_path_buf(),
            records: segment.num_docs() as usize,
            index_metadata: TantivyMeta {
                segment_id: segment.id().uuid_string(),
            },
        })
    }
}

pub struct RelationSearcher {
    pub reader: RelationsReaderService,
}

impl RelationSearcher {
    #[instrument(name = "relation::open", skip_all)]
    pub fn open(config: RelationConfig, open_index: impl OpenIndexMetadata<TantivyMeta>) -> anyhow::Result<Self> {
        if config.version != 2 {
            return Err(anyhow!("Unsupported nidx_relation version"));
        }
        let schema = RelationSchema::new(config.version);
        let deletions_query = RelationDeletionQueryBuilder::new(&schema);
        let index = open_index_with_deletions(schema.schema, open_index, deletions_query)?;

        Ok(Self {
            reader: RelationsReaderService {
                index: index.clone(),
                schema: RelationSchema::new(config.version),
                reader: index
                    .reader_builder()
                    .reload_policy(tantivy::ReloadPolicy::Manual)
                    .try_into()?,
            },
        })
    }

    #[instrument(name = "relation::search", skip_all)]
    pub fn graph_search(
        &self,
        request: &GraphSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<GraphSearchResponse> {
        self.reader.graph_search(request, prefilter)
    }

    #[instrument(name = "relation::suggest", skip_all)]
    pub fn suggest(&self, prefixes: Vec<String>) -> anyhow::Result<Vec<RelationNode>> {
        let subqueries: Vec<_> = prefixes
            .into_iter()
            .filter(|prefix| prefix.len() >= MIN_SUGGEST_PREFIX_LENGTH)
            .map(|prefix| graph_query::PathQuery {
                query: Some(path_query::Query::Path(graph_query::Path {
                    source: Some(graph_query::Node {
                        value: Some(prefix),
                        node_type: Some(NodeType::Entity.into()),
                        new_match_kind: Some(NewMatchKind::Fuzzy(FuzzyMatch {
                            kind: MatchLocation::Prefix.into(),
                            distance: FUZZY_DISTANCE as u32,
                        })),
                        ..Default::default()
                    }),
                    undirected: true,
                    ..Default::default()
                })),
            })
            .collect();

        if subqueries.is_empty() {
            return Ok(vec![]);
        }

        let request = GraphSearchRequest {
            kind: QueryKind::Nodes.into(),
            top_k: NUMBER_OF_RESULTS_SUGGEST as u32,
            query: Some(nidx_protos::GraphQuery {
                path: Some(graph_query::PathQuery {
                    query: Some(path_query::Query::BoolOr(graph_query::BoolQuery {
                        operands: subqueries,
                    })),
                }),
            }),
            ..Default::default()
        };
        let response = self.graph_search(&request, &PrefilterResult::All)?;
        Ok(response.nodes)
    }

    pub fn space_usage(&self) -> usize {
        let usage = self.reader.reader.searcher().space_usage();
        if let Ok(usage) = usage {
            usage.total().get_bytes() as usize
        } else {
            0
        }
    }
}
