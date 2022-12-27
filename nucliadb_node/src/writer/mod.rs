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
    #[tracing::instrument(skip_all)]
    pub fn shutdown(&mut self) {
        for (shard_id, shard) in self.cache.iter_mut() {
            info!("Stopping shard {}", shard_id);
            ShardWriterService::stop(shard);
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn load_shards(&mut self) -> ServiceResult<()> {
        let shards_path = Configuration::shards_path();
        info!("Recovering shards from {shards_path:?}...");
        for entry in std::fs::read_dir(&shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            let shard = ShardWriterService::open(file_name.clone(), &shard_path)?;
            self.cache.insert(file_name, shard);
            info!("Shard loaded: {shard_path:?}");
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn load_shard(&mut self, shard_id: &ShardId) {
        let shard_name = shard_id.id.clone();
        let shard_path = Configuration::shards_path_id(&shard_id.id);
        if self.cache.contains_key(&shard_id.id) {
            info!("Shard {shard_path:?} is already on memory");
            return;
        }
        if !shard_path.is_dir() {
            error!("Shard {shard_path:?} is not on disk");
            return;
        }
        let Ok(shard) = ShardWriterService::open(shard_name, &shard_path) else {
            error!("Shard {shard_path:?} could not be loaded from disk");
            return;
        };
        self.cache.insert(shard_id.id.clone(), shard);
        info!("{shard_path:?}: Shard loaded");
    }
    #[tracing::instrument(skip_all)]
    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&ShardWriterService> {
        self.cache.get(&shard_id.id)
    }
    #[tracing::instrument(skip_all)]
    pub fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardWriterService> {
        self.cache.get_mut(&shard_id.id)
    }

    #[tracing::instrument(skip_all)]
    pub fn new_shard(&mut self) -> ShardCreated {
        let shard_id = Uuid::new_v4().to_string();
        let shard_path = Configuration::shards_path_id(&shard_id);
        let new_shard = ShardWriterService::new(shard_id.clone(), &shard_path).unwrap();
        let data = ShardCreated {
            id: new_shard.id.clone(),
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        };
        self.cache.insert(shard_id, new_shard);
        data
    }
    #[tracing::instrument(skip_all)]
    pub fn delete_shard(&mut self, shard_id: &ShardId) -> ServiceResult<()> {
        self.cache.remove(&shard_id.id);
        let shard_path = Configuration::shards_path_id(&shard_id.id);
        let shard_path = Path::new(&shard_path);
        if shard_path.is_dir() {
            info!("Deleting {:?}", shard_path);
            std::fs::remove_dir_all(shard_path)?;
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn clean_and_upgrade_shard(&mut self, shard_id: &ShardId) -> ServiceResult<ShardCleaned> {
        self.delete_shard(shard_id)?;
        let shard_name = shard_id.id.clone();
        let shard_path = Configuration::shards_path_id(&shard_id.id);
        let new_shard = ShardWriterService::new(shard_name, &shard_path)?;
        let shard_data = ShardCleaned {
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        };
        self.cache.insert(shard_id.id.clone(), new_shard);
        Ok(shard_data)
    }

    #[tracing::instrument(skip_all)]
    pub fn set_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &Resource,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.set_resource(resource)?;
        Ok(Some(shard.count()))
    }

    #[tracing::instrument(skip_all)]
    pub fn add_vectorset(
        &mut self,
        shard_id: &ShardId,
        setid: &VectorSetId,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.add_vectorset(setid)?;
        Ok(Some(shard.count()))
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(
        &mut self,
        shard_id: &ShardId,
        setid: &VectorSetId,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.remove_vectorset(setid)?;
        Ok(Some(shard.count()))
    }

    #[tracing::instrument(skip_all)]
    pub fn join_relations_graph(
        &mut self,
        shard_id: &ShardId,
        graph: &JoinGraph,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.join_relations_graph(graph)?;
        Ok(Some(shard.count()))
    }

    #[tracing::instrument(skip_all)]
    pub fn delete_relation_nodes(
        &mut self,
        shard_id: &ShardId,
        request: &DeleteGraphNodes,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.delete_relation_nodes(request)?;
        Ok(Some(shard.count()))
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_resource(
        &mut self,
        shard_id: &ShardId,
        resource: &ResourceId,
    ) -> ServiceResult<Option<usize>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        shard.remove_resource(resource)?;
        Ok(Some(shard.count()))
    }
    #[tracing::instrument(skip_all)]
    pub fn gc(&mut self, shard_id: &ShardId) -> ServiceResult<Option<()>> {
        let Some(shard) = self.get_mut_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.gc()?))
    }
    #[tracing::instrument(skip_all)]
    pub fn list_vectorsets(&self, shard_id: &ShardId) -> ServiceResult<Option<Vec<String>>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let shard_response = shard.list_vectorsets()?;
        Ok(Some(shard_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn get_shard_ids(&self) -> ShardIds {
        let ids = self
            .cache
            .keys()
            .cloned()
            .map(|id| ShardId { id })
            .collect();
        ShardIds { ids }
    }
}
