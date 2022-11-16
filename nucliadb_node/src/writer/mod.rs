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

pub mod grpc_driver;
use std::collections::HashMap;
use std::path::Path;

use nucliadb_protos::{Resource, ResourceId, ShardCleaned, ShardCreated, ShardId, ShardIds};
use nucliadb_services::*;
use tracing::*;
use uuid::Uuid;

use crate::config::Configuration;
use crate::services::writer::ShardWriterService;
use crate::utils::POOL;

#[derive(Debug)]
pub struct NodeWriterService {
    pub cache: HashMap<String, ShardWriterService>,
}

impl Default for NodeWriterService {
    fn default() -> Self {
        Self::new()
    }
}
impl NodeWriterService {
    pub fn new() -> NodeWriterService {
        NodeWriterService {
            cache: HashMap::new(),
        }
    }
    pub fn shutdown(&mut self) {
        for (shard_id, shard) in self.cache.iter_mut() {
            info!("Stopping shard {}", shard_id);
            ShardWriterService::stop(shard);
        }
    }

    pub fn load_shards(&mut self) -> ServiceResult<()> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());

            let shard = POOL.install(|| ShardWriterService::open(&shard_id.to_string()))?;
            self.cache.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    #[instrument(name = "NodeWriterService::load_shard", skip(self))]
    pub fn load_shard(&mut self, shard_id: &ShardId) {
        info!("{}: Loading shard", shard_id.id);
        let in_memory = self.cache.contains_key(&shard_id.id);
        if !in_memory {
            info!("{}: Shard was not in memory", shard_id.id);
            let in_disk = Path::new(&Configuration::shards_path_id(&shard_id.id)).exists();
            if in_disk {
                info!("{}: Shard was in disk", shard_id.id);
                let shard = POOL
                    .install(|| ShardWriterService::open(&shard_id.id))
                    .unwrap();
                info!("{}: Loaded shard", shard_id.id);
                self.cache.insert(shard_id.id.clone(), shard);
                info!("{}: Inserted on memory", shard_id.id);
            }
        } else {
            info!("{}: Shard was in memory", shard_id.id);
        }
    }

    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&ShardWriterService> {
        self.cache.get(&shard_id.id)
    }
    pub fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardWriterService> {
        self.cache.get_mut(&shard_id.id)
    }

    pub fn new_shard(&mut self) -> ShardCreated {
        let new_id = Uuid::new_v4().to_string();
        let new_shard = POOL.install(|| ShardWriterService::new(&new_id)).unwrap();
        let data = ShardCreated {
            id: new_shard.id.clone(),
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        };
        self.cache.insert(new_id, new_shard);
        data
    }

    pub fn delete_shard(&mut self, shard_id: &ShardId) -> Option<ServiceResult<()>> {
        self.cache
            .remove(&shard_id.id)
            .map(|shard| POOL.install(|| shard.delete()).map_err(|e| e.into()))
    }
    pub fn clean_and_upgrade_shard(&mut self, shard_id: &ShardId) -> ServiceResult<ShardCleaned> {
        self.delete_shard(shard_id).transpose()?;
        let id = &shard_id.id;
        let new_shard = POOL.install(|| ShardWriterService::new(id))?;
        let shard_data = ShardCleaned {
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        };
        self.cache.insert(id.clone(), new_shard);
        Ok(shard_data)
    }
    pub fn set_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &Resource,
    ) -> Option<ServiceResult<usize>> {
        self.get_mut_shard(shard_id).map(|shard| {
            POOL.install(|| shard.set_resource(resource))
                .map(|_| shard.count())
                .map_err(|e| e.into())
        })
    }

    pub fn remove_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &ResourceId,
    ) -> Option<ServiceResult<usize>> {
        self.get_mut_shard(shard_id).map(|shard| {
            POOL.install(|| shard.remove_resource(resource))
                .map(|_| shard.count())
                .map_err(|e| e.into())
        })
    }

    pub fn get_shard_ids(&self) -> ShardIds {
        let ids = self
            .cache
            .keys()
            .cloned()
            .map(|id| ShardId { id })
            .collect();
        ShardIds { ids }
    }

    pub fn gc(&mut self, shard_id: &ShardId) -> Option<ServiceResult<()>> {
        self.get_mut_shard(shard_id)
            .map(|shard| POOL.install(|| shard.gc()).map_err(|e| e.into()))
    }
}
