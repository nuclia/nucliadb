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
use std::time::SystemTime;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_procs::measure;

use super::bfs::GrpcGuide;
use super::utils::*;
use crate::index::*;
use crate::relations_io;

pub struct RelationsReaderService {
    rmode: RMode,
    index: Index,
}
impl Debug for RelationsReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationsReaderService").finish()
    }
}

impl RelationsReaderService {
    #[tracing::instrument(skip_all)]
    fn graph_search(
        &self,
        request: &RelationSearchRequest,
    ) -> NodeResult<Option<EntitiesSubgraphResponse>> {
        let Some(bfs_request) = request.subgraph.as_ref() else {
            return Ok(None);
        };

        let id = Some(&request.shard_id);
        let time = SystemTime::now();
        let reader = self.index.start_reading()?;
        let depth = bfs_request.depth.map(|v| v as usize).unwrap_or(usize::MAX);
        let mut entry_points = Vec::with_capacity(bfs_request.entry_points.len());

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} -  Creating entry points: starts {v} ms");
        }
        for node in bfs_request.entry_points.iter() {
            let name = node.value.clone();
            let type_info = node_type_parsing(node.ntype(), &node.subtype);
            let xtype = type_info.0.to_string();
            let subtype = type_info.1.map(|s| s.to_string());
            let node = IoNode::new(name, xtype, subtype);
            match reader.get_node_id(node.hash()) {
                Ok(None) => (),
                Ok(Some(id)) => entry_points.push(id),
                Err(e) => error!("{e:?} during {node:?}"),
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} -  Creating entry points: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - adding query type filters: starts {v} ms");
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - adding query type filters: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - running the search: starts {v} ms");
        }
        let guide = GrpcGuide {
            node_filters: HashSet::new(),
            edge_filters: HashSet::new(),
            reader: &reader,
            jump_always: dictionary::SYNONYM,
        };
        let mut subgraph = vec![];
        for i in reader.search(guide, depth, entry_points)? {
            let from = reader.get_node(i.from()).map(|node| RelationNode {
                value: node.name().to_string(),
                subtype: node.subtype().map(|s| s.to_string()).unwrap_or_default(),
                ntype: string_to_node_type(node.xtype()) as i32,
            })?;

            let to = reader.get_node(i.to()).map(|node| RelationNode {
                value: node.name().to_string(),
                subtype: node.subtype().map(|s| s.to_string()).unwrap_or_default(),
                ntype: string_to_node_type(node.xtype()) as i32,
            })?;
            let relation_metadata = reader.get_edge_metadata(i.edge())?;
            let relation = reader.get_edge(i.edge()).map(|edge| Relation {
                to: Some(to),
                source: Some(from),
                relation: string_to_rtype(edge.xtype()) as i32,
                metadata: relation_metadata.map(RelationMetadata::from),
                relation_label: edge.subtype().map(|s| s.to_string()).unwrap_or_default(),
            })?;
            subgraph.push(relation);
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - running the search: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms");
        }
        Ok(Some(EntitiesSubgraphResponse {
            relations: subgraph,
        }))
    }

    #[tracing::instrument(skip_all)]
    fn prefix_search(
        &self,
        request: &RelationSearchRequest,
    ) -> NodeResult<Option<RelationPrefixSearchResponse>> {
        use crate::bfs_engine::BfsGuide;
        let Some(prefix_request) = request.prefix.as_ref() else {
            return Ok(None);
        };

        let id = Some(&request.shard_id);
        let time = SystemTime::now();
        let prefix = &prefix_request.prefix;
        let reader = self.index.start_reading()?;

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - running prefix search: starts {v} ms");
        }
        let prefixes = reader
            .prefix_search(&self.rmode, prefix)?
            .into_iter()
            .flat_map(|key| reader.get_node_id(&key).ok().flatten());
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - running prefix search: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - generating results: starts {v} ms");
        }

        let mut node_filters = HashSet::new();
        prefix_request.node_filters.iter().for_each(|filter| {
            let node_type = filter.node_type();
            let node_subtype = filter
                .node_subtype
                .as_ref()
                .map_or_else(|| "", |subtype| subtype);
            let type_info = node_type_parsing(node_type, node_subtype);
            node_filters.insert(type_info);
        });

        let guide = GrpcGuide {
            node_filters,
            edge_filters: HashSet::new(),
            reader: &reader,
            jump_always: dictionary::SYNONYM,
        };

        let nodes = prefixes
            .into_iter()
            .filter(|n| guide.node_allowed(*n))
            .map(|id| {
                reader.get_node(id).map(|node| RelationNode {
                    value: node.name().to_string(),
                    subtype: node.subtype().map(|s| s.to_string()).unwrap_or_default(),
                    ntype: string_to_node_type(node.xtype()) as i32,
                })
            });
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - generating results: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms");
        }
        Ok(Some(RelationPrefixSearchResponse {
            nodes: nodes.collect::<Result<Vec<_>, _>>()?,
        }))
    }

    #[tracing::instrument(skip_all)]
    pub fn reload(&self) {
        let _v = self
            .index
            .start_reading()
            .and_then(|reader| reader.reload(&self.rmode))
            .map_err(|err| error!("Reload error {err:?}"));
    }
}

impl RelationReader for RelationsReaderService {
    #[measure(actor = "relations", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        Ok(self
            .index
            .start_reading()
            .and_then(|reader| reader.no_nodes())
            .map(|v| v as usize)?)
    }

    #[measure(actor = "relations", metric = "get_edges")]
    #[tracing::instrument(skip_all)]
    fn get_edges(&self) -> NodeResult<EdgeList> {
        let time = SystemTime::now();

        let id: Option<String> = None;
        let reader = self.index.start_reading()?;
        let iter = reader.iter_edge_ids()?;
        let mut edges = Vec::new();
        let mut found = HashSet::new();
        for id in iter {
            let id = id?;
            let edge = reader.get_edge(id)?;
            let xtype = edge.xtype();
            let subtype = edge
                .subtype()
                .map_or_else(String::default, |s| s.to_string());
            let hash = relations_io::compute_hash(&[xtype.as_bytes(), subtype.as_bytes()]);
            if found.insert(hash) {
                edges.push(RelationEdge {
                    edge_type: string_to_rtype(xtype) as i32,
                    property: subtype,
                });
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms");
        }

        Ok(EdgeList { list: edges })
    }

    #[measure(actor = "relations", metric = "get_node_types")]
    #[tracing::instrument(skip_all)]
    fn get_node_types(&self) -> NodeResult<TypeList> {
        let time = SystemTime::now();

        let id: Option<String> = None;
        let mut found = HashSet::new();
        let mut types = Vec::new();
        let reader = self.index.start_reading()?;
        let iter = reader.iter_node_ids()?;
        for id in iter {
            let id = id?;
            let node = reader.get_node(id)?;
            let xtype = node.xtype();
            let subtype = node
                .subtype()
                .map_or_else(String::default, |s| s.to_string());
            let hash = relations_io::compute_hash(&[xtype.as_bytes(), subtype.as_bytes()]);
            if found.insert(hash) {
                types.push(RelationTypeListMember {
                    with_type: string_to_node_type(xtype) as i32,
                    with_subtype: subtype,
                });
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Ending at {v} ms");
        }

        Ok(TypeList { list: types })
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
        let reader = self.index.start_reading()?;
        let ids = reader
            .iter_node_ids()?
            .filter_map(|node| node.ok())
            .filter_map(|id| reader.get_node(id).ok())
            .map(|s| format!("{s:?}"))
            .collect();

        Ok(ids)
    }
}

impl RelationsReaderService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &RelationConfig) -> NodeResult<Self> {
        if !config.path.exists() {
            return Err(node_error!("Shard does not exist".to_string()));
        }
        let (index, rmode) = Index::new_reader(&config.path)?;
        Ok(RelationsReaderService { index, rmode })
    }
}
