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

use nucliadb_protos::relation::RelationType;
use nucliadb_protos::*;
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::graph::*;
use crate::search_engine::*;
use crate::service::utils::*;

pub struct RelationsReaderService {
    index: StorageSystem,
}
impl Debug for RelationsReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationReaderService").finish()
    }
}

impl RelationReader for RelationsReaderService {
    fn get_edges(&self) -> EdgeList {
        let list: Vec<_> = get_edge_types(&self.index)
            .into_iter()
            .map(|rtype| string_to_rtype(&rtype))
            .map(|(etype, property)| RelationEdge {
                property,
                edge_type: etype as i32,
            })
            .collect();
        EdgeList { list }
    }
    fn get_node_types(&self) -> TypeList {
        let list: Vec<_> = get_node_types(&self.index)
            .into_iter()
            .map(|(node_type, subtype)| RelationTypeListMember {
                with_type: string_to_node_type(&node_type) as i32,
                with_subtype: subtype,
            })
            .collect();
        TypeList { list }
    }
}

impl ReaderChild for RelationsReaderService {
    type Request = RelationSearchRequest;
    type Response = RelationSearchResponse;

    fn stop(&self) -> InternalResult<()> {
        info!("Stopping relation reader Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let txn = self.index.ro_txn();
        let count = self.index.no_nodes(&txn);
        txn.commit().unwrap();
        count as usize
    }
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        use std::collections::HashSet;
        let txn = self.index.ro_txn();
        let entry_points: Vec<_> = request
            .entry_points
            .iter()
            .map(|node| {
                NodeBuilder::new()
                    .with_value(node.value.clone())
                    .with_type(node_type_parsing(node.ntype()))
                    .with_subtype(node.subtype.clone())
                    .build()
                    .to_string()
            })
            .map(|node| self.index.get_id(&txn, &node))
            .filter(|n| n.is_some())
            .flatten()
            .collect();
        let mut query = QueryConstructor::default()
            .depth(request.depth as u32)
            .prefixed(request.prefix.clone())
            .always_jump(HashSet::from([rtype_parsing(RelationType::Synonym, "")]))
            .build()
            .unwrap();
        request
            .type_filters
            .iter()
            .cloned()
            .for_each(|tf| query.add_types(node_type_parsing(tf.ntype()), tf.subtype));
        Ok(RelationSearchResponse {
            neighbours: process_query(&entry_points, &self.index, query)
                .matches
                .into_iter()
                .map(|id| Node::from(self.index.get_node(&txn, id).unwrap()))
                .map(|node| RelationNode {
                    value: node.get_value().to_string(),
                    ntype: string_to_node_type(node.get_type()) as i32,
                    subtype: node.get_subtype().to_string(),
                })
                .collect(),
        })
    }
    fn stored_ids(&self) -> Vec<String> {
        let txn = self.index.ro_txn();
        let keys: Vec<_> = self.index.get_keys(&txn).collect();
        txn.commit().unwrap();
        keys
    }
    fn reload(&self) {}
}

impl RelationsReaderService {
    pub fn start(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(RelationsReaderService::new(config).unwrap())
        } else {
            Ok(RelationsReaderService::open(config).unwrap())
        }
    }
    pub fn new(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            std::fs::create_dir_all(path).unwrap();

            Ok(RelationsReaderService {
                index: StorageSystem::create(path),
            })
        }
    }

    pub fn open(config: &RelationConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(RelationsReaderService {
                index: StorageSystem::create(path),
            })
        }
    }
}
