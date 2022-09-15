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

use nucliadb_protos::{Resource, ResourceId, ShardCreated, ShardId, ShardIds};
use nucliadb_services::*;
use tracing::*;
use uuid::Uuid;

use crate::config::Configuration;
use crate::services::writer::ShardWriterService;

type ShardWriterDB = HashMap<String, ShardWriterService>;

/// Stores the shards internal memory database
#[derive(Debug)]
pub struct NodeWriterService {
    /// The hashmap of shards on memory
    pub shards: ShardWriterDB,
}

impl Default for NodeWriterService {
    fn default() -> Self {
        Self::new()
    }
}
impl NodeWriterService {
    pub fn new() -> NodeWriterService {
        NodeWriterService {
            shards: ShardWriterDB::new(),
        }
    }
    /// Stop all shards on memory
    pub async fn shutdown(&mut self) {
        for (shard_id, shard) in self.shards.iter_mut() {
            info!("Stopping shard {}", shard_id);
            ShardWriterService::stop(shard).await;
        }
    }

    pub async fn load_shards(&mut self) -> ServiceResult<()> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());

            let shard: ShardWriterService = ShardWriterService::open(&shard_id.to_string()).await?;
            self.shards.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    #[instrument(name = "NodeWriterService::load_shard", skip(self))]
    async fn load_shard(&mut self, shard_id: &ShardId) {
        info!("{}: Loading shard", shard_id.id);
        let in_memory = self.shards.contains_key(&shard_id.id);
        if !in_memory {
            info!("{}: Shard was not in memory", shard_id.id);
            let in_disk = Path::new(&Configuration::shards_path_id(&shard_id.id)).exists();
            if in_disk {
                info!("{}: Shard was in disk", shard_id.id);
                let shard = ShardWriterService::open(&shard_id.id)
                    .instrument(trace_span!("ShardWriterService::open"))
                    .await
                    .unwrap();
                info!("{}: Loaded shard", shard_id.id);
                self.shards.insert(shard_id.id.clone(), shard);
                info!("{}: Inserted on memory", shard_id.id);
            }
        } else {
            info!("{}: Shard was in memory", shard_id.id);
        }
    }

    pub async fn get_shard(&mut self, shard_id: &ShardId) -> Option<&ShardWriterService> {
        self.load_shard(shard_id).await;
        self.shards.get(&shard_id.id)
    }
    pub async fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardWriterService> {
        self.load_shard(shard_id).await;
        self.shards.get_mut(&shard_id.id)
    }

    pub async fn new_shard(&mut self) -> ShardCreated {
        let new_id = Uuid::new_v4().to_string();
        let new_shard = ShardWriterService::new(&new_id).await.unwrap();
        let prev = self.shards.insert(new_id.clone(), new_shard);
        debug_assert!(prev.is_none());
        let new_shard = self.get_shard(&ShardId { id: new_id }).await.unwrap();
        ShardCreated {
            id: new_shard.id.clone(),
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        }
    }

    pub async fn delete_shard(&mut self, shard_id: &ShardId) -> Option<ServiceResult<()>> {
        self.load_shard(shard_id).await;
        if let Some(shard) = self.shards.remove(&shard_id.id) {
            Some(shard.delete().await.map_err(|e| e.into()))
        } else {
            None
        }
    }

    pub async fn set_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &Resource,
    ) -> Option<ServiceResult<usize>> {
        self.load_shard(shard_id).await;
        if let Some(shard) = self.get_mut_shard(shard_id).await {
            let res = shard
                .set_resource(resource)
                .await
                .map(|_| shard.count())
                .map_err(|e| e.into());
            Some(res)
        } else {
            None
        }
    }

    pub async fn remove_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &ResourceId,
    ) -> Option<ServiceResult<usize>> {
        self.load_shard(shard_id).await;
        if let Some(shard) = self.get_mut_shard(shard_id).await {
            let res = shard
                .remove_resource(resource)
                .await
                .map(|_| shard.count())
                .map_err(|e| e.into());
            Some(res)
        } else {
            None
        }
    }

    pub fn get_shard_ids(&self) -> ShardIds {
        let ids = self
            .shards
            .keys()
            .cloned()
            .map(|id| ShardId { id })
            .collect();
        ShardIds { ids }
    }

    pub async fn gc(&mut self, shard_id: &ShardId) -> Option<ServiceResult<()>> {
        self.load_shard(shard_id).await;
        match self.get_mut_shard(shard_id).await {
            Some(shard) => Some(shard.gc().await.map_err(|e| e.into())),
            None => None,
        }
    }
}
