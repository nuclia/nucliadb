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

use nucliadb_core::prelude::*;
use nucliadb_core::protos::{
    DeleteGraphNodes, JoinGraph, NewShardRequest, NewVectorSetRequest, OpStatus, Resource,
    ResourceId, ShardCleaned, ShardCreated, ShardId, ShardIds, VectorSetId,
};
use nucliadb_core::thread::ThreadPoolBuilder;
use nucliadb_core::tracing::{self, *};
use nucliadb_vectors::data_point_provider::Merger as VectorsMerger;
use uuid::Uuid;

use crate::env;
use crate::services::writer::ShardWriterService;

#[derive(Debug, Default)]
pub struct NodeWriterService {
    pub cache: HashMap<String, ShardWriterService>,
}

impl NodeWriterService {
    fn initialize_file_system() -> NodeResult<()> {
        let shards_path = env::shards_path();
        let data_path = env::data_path();
        if !data_path.exists() {
            return Err(node_error!("{:?} is not created", data_path));
        }
        if !shards_path.exists() {
            std::fs::create_dir(&shards_path)?;
        }
        Ok(())
    }
    pub fn new() -> NodeResult<Self> {
        Self::initialize_file_system()?;
        // We shallow the error if the threadpools were already initialized
        let _ = ThreadPoolBuilder::new().num_threads(10).build_global();
        let _ = VectorsMerger::install_global().map(std::thread::spawn);
        Ok(Self::default())
    }

    #[tracing::instrument(skip_all)]
    pub fn shutdown(&self) {
        for (shard_id, shard) in self.cache.iter() {
            debug!("Stopping shard {}", shard_id);
            shard.stop();
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn load_shards(&mut self) -> NodeResult<()> {
        let shards_path = env::shards_path();
        debug!("Recovering shards from {shards_path:?}...");
        for entry in std::fs::read_dir(&shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            match ShardWriterService::open(file_name.clone(), &shard_path) {
                Err(err) => error!("Shard {shard_path:?} could not be loaded from disk: {err:?}"),
                Ok(shard) => {
                    debug!("{shard_path:?}: Shard loaded");
                    self.cache.insert(file_name, shard);
                }
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn load_shard(&mut self, shard_id: &ShardId) {
        let shard_name = shard_id.id.clone();
        let shard_path = env::shards_path_id(&shard_id.id);
        if self.cache.contains_key(&shard_id.id) {
            debug!("Shard {shard_path:?} is already on memory");
            return;
        }
        if !shard_path.is_dir() {
            error!("Shard {shard_path:?} is not on disk");
            return;
        }
        match ShardWriterService::open(shard_name, &shard_path) {
            Err(err) => error!("Shard {shard_path:?} could not be loaded from disk: {err:?}"),
            Ok(shard) => {
                self.cache.insert(shard_id.id.clone(), shard);
                debug!("{shard_path:?}: Shard loaded");
            }
        }
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
    pub fn new_shard(request: &NewShardRequest) -> NodeResult<ShardCreated> {
        let shard_id = Uuid::new_v4().to_string();
        let shard_path = env::shards_path_id(&shard_id);
        let new_shard = ShardWriterService::new(shard_id.clone(), &shard_path, request)?;
        Ok(ShardCreated {
            id: shard_id,
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn delete_shard(&mut self, shard_id: &ShardId) -> NodeResult<()> {
        if shard_id.id.is_empty() {
            warn!("Shard id is empty");
            return Ok(());
        }

        self.cache.remove(&shard_id.id);

        let shard_path = env::shards_path_id(&shard_id.id);
        if shard_path.exists() {
            debug!("Deleting {:?}", shard_path);
            std::fs::remove_dir_all(shard_path)?;
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn clean_and_upgrade_shard(&mut self, shard_id: &ShardId) -> NodeResult<ShardCleaned> {
        self.cache.remove(&shard_id.id);
        let shard_id = shard_id.id.clone();
        let shard_path = env::shards_path_id(&shard_id);
        let new_shard = ShardWriterService::clean_and_create(shard_id.clone(), &shard_path)?;
        let shard_data = ShardCleaned {
            document_service: new_shard.document_version() as i32,
            paragraph_service: new_shard.paragraph_version() as i32,
            vector_service: new_shard.vector_version() as i32,
            relation_service: new_shard.relation_version() as i32,
        };
        self.cache.insert(shard_id, new_shard);
        Ok(shard_data)
    }

    #[tracing::instrument(skip_all)]
    pub fn set_resource(
        &self,
        shard_id: &ShardId,
        resource: &Resource,
    ) -> NodeResult<Option<OpStatus>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };

        shard.set_resource(resource)?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn add_vectorset(&self, request: &NewVectorSetRequest) -> NodeResult<Option<OpStatus>> {
        let Some(setid) = &request.id else {
            return Err(node_error!("missing vectorset id"));
        };
        let Some(shard_id) = &setid.shard else {
            return Err(node_error!("missing shard id"));
        };
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        shard.add_vectorset(setid, request.similarity())?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(
        &self,
        shard_id: &ShardId,
        setid: &VectorSetId,
    ) -> NodeResult<Option<OpStatus>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        shard.remove_vectorset(setid)?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn join_relations_graph(
        &self,
        shard_id: &ShardId,
        graph: &JoinGraph,
    ) -> NodeResult<Option<OpStatus>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        shard.join_relations_graph(graph)?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn delete_relation_nodes(
        &self,
        shard_id: &ShardId,
        request: &DeleteGraphNodes,
    ) -> NodeResult<Option<OpStatus>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        shard.delete_relation_nodes(request)?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_resource(
        &self,
        shard_id: &ShardId,
        resource: &ResourceId,
    ) -> NodeResult<Option<OpStatus>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        shard.remove_resource(resource)?;
        Ok(Some(shard.get_opstatus()?))
    }

    #[tracing::instrument(skip_all)]
    pub fn gc(&self, shard_id: &ShardId) -> NodeResult<Option<()>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.gc()?))
    }
    #[tracing::instrument(skip_all)]
    pub fn list_vectorsets(&self, shard_id: &ShardId) -> NodeResult<Option<Vec<String>>> {
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
