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

use nucliadb_protos::*;
use nucliadb_service_interface::prelude::*;
use nucliadb_service_interface::prelude::nucliadb_protos::relation_neighbours_search_request::EntryPoint;
use tracing::*;

use super::bfs::GrpcGuide;
use super::utils::*;
use crate::relations::index::*;
use crate::relations::relations_io;

pub struct RelationsReaderService {
    rmode: RMode,
    index: Index,
}
impl Debug for RelationsReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationReaderService").finish()
    }
}

impl RelationsReaderService {
    const PREFIX_RESULTS_LIMIT: usize = 10;

    fn tic(start: &SystemTime, log: String) {
        if let Ok(elapsed) = start.elapsed().map(|elapsed| elapsed.as_millis()) {
            info!("{}", format!("{log} {elapsed} ms"));
        }
    }

    fn get_entry_points(reader: &GraphReader, entry_points: &Vec<EntryPoint>) -> Vec<(Entity, usize)> {
        let mut parsed = Vec::with_capacity(entry_points.len());

        for entry_point in entry_points.iter() {
            if entry_point.node.is_none() {
                continue;
            }
            let relation_node = entry_point.node.as_ref().unwrap();
            let depth = entry_point.depth;

            let name = relation_node.value.clone();
            let type_info = node_type_parsing(relation_node.ntype(), &relation_node.subtype);
            let xtype = type_info.0.to_string();
            let subtype = type_info.1.map(|s| s.to_string());
            let node = IoNode::new(name, xtype, subtype);

            match reader.get_node_id(node.hash()) {
                Ok(Some(entity)) => parsed.push((entity, depth as usize)),
                Ok(None) => (),
                Err(e) => error!("{e:?} during {node:?}"),
            }
        }

        parsed
    }

    #[tracing::instrument(skip_all)]
    fn neighbours_search(
        &self,
        shard_id: &String,
        request: &RelationNeighboursSearchRequest,
    ) -> InternalResult<Vec<RelationNeighbours>> {
        let reader = self.index.start_reading()?;
        let time = SystemTime::now();

        Self::tic(&time, format!("{shard_id:?} - Creating entry points: starts"));
        let entry_points = Self::get_entry_points(&reader, &request.entry_points);
        Self::tic(&time, format!("{shard_id:?} - Creating entry points: ends"));

        Self::tic(&time, format!("{shard_id:?} - adding query type filters: starts"));
        let type_filters = request.type_filters
            .iter()
            .map(|filter| node_type_parsing(filter.ntype(), &filter.subtype))
            .collect::<HashSet<_>>();
        Self::tic(&time, format!("{shard_id:?} - adding query type filters: ends"));

        let guide = GrpcGuide {
            type_filters,
            reader: &reader,
            jump_always: dictionary::SYNONYM,
        };

        let mut response = vec![];

        for (entity, depth) in entry_points {
            Self::tic(&time, format!("{shard_id:?} - running the search (for {entity:?}): starts"));
            // TODO: guide should be passed as a ref?
            let neighbours = reader.search(guide.clone(), depth, vec![entity])?;
            Self::tic(&time, format!("{shard_id:?} - running the search (for {entity:?}): ends"));

            Self::tic(&time, format!("{shard_id:?} - producing results (for {entity:?}): starts"));
            let neighbours = neighbours
                .into_iter()
                .map(|entity| {
                    reader.get_node(entity)
                        .map(|ionode| RelationNode {
                            value: ionode.name().to_string(),
                            ntype: string_to_node_type(ionode.xtype()) as i32,
                            subtype: ionode.subtype().map(|s| s.to_string()).unwrap_or_default(),
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Self::tic(&time, format!("{shard_id:?} - producing results (for {entity:?}): ends"));

            response.push(RelationNeighbours {
                neighbours
            })
        }

        Self::tic(&time, format!("{shard_id:?} - Ending at"));
        Ok(response)
    }

    /// Relation search by prefix
    #[tracing::instrument(skip_all)]
    fn prefix_search(
        &self,
        shard_id: &String,
        request: &RelationPrefixSearchRequest,
    ) -> InternalResult<RelationPrefixSearchResponse> {
        let prefix = &request.prefix;
        let reader = self.index.start_reading()?;
        let time = SystemTime::now();

        Self::tic(&time, format!("{shard_id:?} - running prefix search: starts"));
        let prefixes = reader
            .prefix_search(&self.rmode, Self::PREFIX_RESULTS_LIMIT, prefix)?
            .into_iter()
            .flat_map(|key| reader.get_node_id(&key).ok().flatten());
        Self::tic(&time, format!("{shard_id:?} - running prefix search: ends"));

        Self::tic(&time, format!("{shard_id:?} - generating results: starts"));
        let nodes = prefixes
            .into_iter()
            .map(|id| {
                reader
                    .get_node(id)
                    .map(|node| RelationNode {
                        value: node.name().to_string(),
                        subtype: node.subtype().map(|s| s.to_string()).unwrap_or_default(),
                        ntype: string_to_node_type(node.xtype()) as i32,
                    })
            });
        Self::tic(&time, format!("{shard_id:?} - generating results: ends"));

        Self::tic(&time, format!("{shard_id:?} - Ending at"));
        Ok(RelationPrefixSearchResponse {
            nodes: nodes.collect::<Result<Vec<_>, _>>()?,
        })
    }

}
impl RelationReader for RelationsReaderService {
    #[tracing::instrument(skip_all)]
    fn count(&self) -> InternalResult<usize> {
        Ok(self
            .index
            .start_reading()
            .and_then(|reader| reader.no_nodes())
            .map(|v| v as usize)?)
    }
    #[tracing::instrument(skip_all)]
    fn get_edges(&self) -> InternalResult<EdgeList> {
        let id: Option<String> = None;
        let time = SystemTime::now();
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
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(EdgeList { list: edges })
    }
    #[tracing::instrument(skip_all)]
    fn get_node_types(&self) -> InternalResult<TypeList> {
        let id: Option<String> = None;
        let time = SystemTime::now();
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
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(TypeList { list: types })
    }
}

impl ReaderChild for RelationsReaderService {
    type Request = RelationSearchRequest;
    type Response = RelationSearchResponse;
    #[tracing::instrument(skip_all)]
    fn stop(&self) -> InternalResult<()> {
        info!("Stopping relation reader Service");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        let mut response = Self::Response {
            prefix: None,
            neighbours: vec![],
        };
        let shard_id = &request.shard_id;

        if let Some(ref prefix_request) = request.prefix {
            info!("{shard_id:?} - Prefix search");
            response.prefix = Some(self.prefix_search(&shard_id, prefix_request)?);
        }

        if let Some(ref neighbours_request) = request.neighbours {
            info!("{shard_id:?} - Neighbours search");
            response.neighbours = self.neighbours_search(&shard_id, neighbours_request)?;
        }

        // TODO: implement relation path request

        Ok(response)
    }
    #[tracing::instrument(skip_all)]
    fn stored_ids(&self) -> Vec<String> {
        let mut list = vec![];
        if let Ok(reader) = self.index.start_reading() {
            if let Ok(iter) = reader.iter_node_ids() {
                iter.filter_map(|node| node.ok())
                    .filter_map(|id| reader.get_node(id).ok())
                    .for_each(|s| list.push(format!("{s:?}")));
            }
        }
        list
    }
    #[tracing::instrument(skip_all)]
    fn reload(&self) {
        let _v = self
            .index
            .start_reading()
            .and_then(|reader| reader.reload(&self.rmode))
            .map_err(|err| error!("Reload error {err:?}"));
    }
}

impl RelationsReaderService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(RelationsReaderService::new(config).unwrap())
        } else {
            Ok(RelationsReaderService::open(config).unwrap())
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn new(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            std::fs::create_dir_all(path).unwrap();
            let (index, rmode) = Index::new_reader(path)?;
            Ok(RelationsReaderService { index, rmode })
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn open(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            let (index, rmode) = Index::new_reader(path)?;
            Ok(RelationsReaderService { index, rmode })
        }
    }
}
