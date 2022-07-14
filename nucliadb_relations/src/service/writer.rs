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

use async_trait::async_trait;
use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::graph::*;
use crate::service::utils::*;

pub struct RelationsWriterService {
    index: StorageSystem,
}

impl WService for RelationsWriterService {}
impl RelationWriterOnly for RelationsWriterService {}
impl RelationServiceWriter for RelationsWriterService {}

impl std::fmt::Debug for RelationsWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RelationWriterService").finish()
    }
}

#[async_trait]
impl ServiceChild for RelationsWriterService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping relation writer Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let txn = self.index.ro_txn();
        let count = self.index.no_nodes(&txn);
        txn.commit().unwrap();
        count as usize
    }
}

impl WriterChild for RelationsWriterService {
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        let mut txn = self.index.rw_txn();
        if let Some(id) = self.index.get_id(&txn, &resource_id.uuid) {
            self.index.delete_node(&mut txn, id);
            txn.commit().unwrap();
            Ok(())
        } else {
            txn.commit().unwrap();
            Err(Box::new(format!("Invalid resource {}", resource_id.uuid)))
        }
    }
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        info!("Set resource in relations starts");
        let mut txn = self.index.rw_txn();
        if resource.status != ResourceStatus::Delete as i32 {
            for relation in &resource.relations {
                let source = relation.source.as_ref().unwrap();
                let to = relation.to.as_ref().unwrap();
                let source = NodeBuilder::new()
                    .with_value(source.value.clone())
                    .with_type(node_type_parsing(source.ntype()))
                    .with_subtype(source.subtype.clone())
                    .build()
                    .to_string();
                let to = NodeBuilder::new()
                    .with_value(to.value.clone())
                    .with_type(node_type_parsing(to.ntype()))
                    .with_subtype(to.subtype.clone())
                    .build()
                    .to_string();
                self.index.add_node(&mut txn, source.clone());
                self.index.add_node(&mut txn, to.clone());
                let source = self.index.get_id(&txn, &source).unwrap();
                let to = self.index.get_id(&txn, &to).unwrap();
                let etype = rtype_parsing(relation.relation(), &relation.relation_label);
                self.index.add_edge(&mut txn, Edge::new(source, etype, to));
            }
        }
        txn.commit().unwrap();
        info!("Set resource in relations ends");
        Ok(())
    }
    fn garbage_collection(&mut self) {}
}

impl RelationsWriterService {
    pub async fn start(config: &RelationServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        Ok(RelationsWriterService {
            index: StorageSystem::start(path),
        })
    }
    pub async fn new(config: &RelationServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(path).await.unwrap();

            Ok(RelationsWriterService {
                index: StorageSystem::create(path),
            })
        }
    }

    pub async fn open(config: &RelationServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(RelationsWriterService {
                index: StorageSystem::open(path),
            })
        }
    }
}
