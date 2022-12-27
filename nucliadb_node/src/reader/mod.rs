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

use nucliadb_protos::{
    DocumentSearchRequest, DocumentSearchResponse, EdgeList, IdCollection, ParagraphSearchRequest,
    ParagraphSearchResponse, SearchRequest, SearchResponse, Shard as ShardPB, ShardId, ShardList,
    SuggestRequest, SuggestResponse, TypeList, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_services::*;
use rayon::prelude::*;
use tracing::*;

use crate::config::Configuration;
use crate::services::reader::ShardReaderService;
use crate::utils::POOL;

#[derive(Debug)]
pub struct NodeReaderService {
    pub cache: HashMap<String, ShardReaderService>,
}

impl Default for NodeReaderService {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeReaderService {
    pub fn new() -> NodeReaderService {
        NodeReaderService {
            cache: HashMap::new(),
        }
    }

    /// Stop all shards on memory
    #[tracing::instrument(skip_all)]
    pub fn shutdown(&mut self) {
        for (shard_id, shard) in &mut self.cache {
            info!("Stopping shard {}", shard_id);
            ShardReaderService::stop(shard);
        }
    }

    /// Load all shards on the shards memory structure
    #[tracing::instrument(skip_all)]
    pub fn load_shards(&mut self) -> ServiceResult<()> {
        let shards_path = Configuration::shards_path();
        info!("Recovering shards from {shards_path:?}...");
        for entry in std::fs::read_dir(&shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            let shard =
                POOL.install(|| ShardReaderService::open(file_name.clone(), &shard_path))?;
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
        let Ok(shard) = POOL.install(|| ShardReaderService::open(shard_name, &shard_path)) else {
            error!("Shard {shard_path:?} could not be loaded from disk");
            return;
        };
        self.cache.insert(shard_id.id.clone(), shard);
        info!("{shard_path:?}: Shard loaded");
    }

    #[tracing::instrument(skip_all)]
    pub fn get_shard(&self, shard_id: &ShardId) -> Option<&ShardReaderService> {
        self.cache.get(&shard_id.id)
    }
    #[tracing::instrument(skip_all)]
    pub fn get_shards(&self) -> ServiceResult<ShardList> {
        let shards = POOL.install(|| {
            self.cache
                .par_iter()
                .map(|(shard_id, shard)| {
                    shard.get_resources().map(|count| ShardPB {
                        shard_id: shard_id.clone(),
                        resources: count as u64,
                        paragraphs: 0_u64,
                        sentences: 0_u64,
                    })
                })
                .collect::<Result<Vec<_>, _>>()
        })?;
        Ok(ShardList { shards })
    }
    #[tracing::instrument(skip_all)]
    pub fn suggest(
        &self,
        shard_id: &ShardId,
        request: SuggestRequest,
    ) -> Option<ServiceResult<SuggestResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.suggest(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn search(
        &self,
        shard_id: &ShardId,
        request: SearchRequest,
    ) -> Option<ServiceResult<SearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_search(
        &self,
        shard_id: &ShardId,
        request: RelationSearchRequest,
    ) -> Option<ServiceResult<RelationSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.relation_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_search(
        &self,
        shard_id: &ShardId,
        request: VectorSearchRequest,
    ) -> Option<ServiceResult<VectorSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.vector_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(
        &self,
        shard_id: &ShardId,
        request: ParagraphSearchRequest,
    ) -> Option<ServiceResult<ParagraphSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.paragraph_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn document_search(
        &self,
        shard_id: &ShardId,
        request: DocumentSearchRequest,
    ) -> Option<ServiceResult<DocumentSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.document_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn document_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_field_keys()))
            .map(|ids| IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_paragraphs_keys()))
            .map(|ids| IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_vectors_keys()))
            .map(|ids| IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_relations_keys()))
            .map(|ids| IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_edges(&self, shard_id: &ShardId) -> Option<ServiceResult<EdgeList>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_relations_edges()))
            .map(|r| r.map_err(|e| e.into()))
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_types(&self, shard_id: &ShardId) -> Option<ServiceResult<TypeList>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.get_relations_types()))
            .map(|r| r.map_err(|e| e.into()))
    }
}
