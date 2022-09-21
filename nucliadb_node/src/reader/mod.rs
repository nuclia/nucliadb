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
type ShardReaderDB = HashMap<String, ShardReaderService>;

#[derive(Debug)]
pub struct NodeReaderService {
    pub shards: ShardReaderDB,
}

impl Default for NodeReaderService {
    fn default() -> Self {
        Self::new()
    }
}

impl NodeReaderService {
    pub fn new() -> NodeReaderService {
        NodeReaderService {
            shards: ShardReaderDB::new(),
        }
    }

    /// Stop all shards on memory
    pub fn shutdown(&mut self) {
        for (shard_id, shard) in &mut self.shards {
            info!("Stopping shard {}", shard_id);
            ShardReaderService::stop(shard);
        }
    }

    /// Load all shards on the shards memory structure
    pub fn load_shards(&mut self) -> ServiceResult<()> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());
            let shard = POOL.install(|| ShardReaderService::open(&shard_id.to_string()))?;
            self.shards.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    #[instrument(name = "NodeReaderService::load_shard", skip(self))]
    pub fn load_shard(&mut self, shard_id: &ShardId) {
        let shard_id = &shard_id.id;
        info!("{}: Loading shard", shard_id);
        let in_memory = self.shards.contains_key(shard_id);
        if !in_memory {
            info!("{}: Shard was not in memory", shard_id);
            let in_disk = Path::new(&Configuration::shards_path_id(shard_id)).exists();
            if in_disk {
                info!("{}: Shard was in disk", shard_id);
                let shard = POOL.install(|| ShardReaderService::open(shard_id)).unwrap();
                info!("{}: Loaded shard", shard_id);
                self.shards.insert(shard_id.to_string(), shard);
                info!("{}: Inserted on memory", shard_id);
            }
        } else {
            info!("{}: Shard was in memory", shard_id);
        }
    }
    pub fn get_shard(&mut self, shard_id: &ShardId) -> Option<&ShardReaderService> {
        self.load_shard(shard_id);
        self.shards.get(&shard_id.id)
    }
    pub fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardReaderService> {
        self.load_shard(shard_id);
        self.shards.get_mut(&shard_id.id)
    }
    pub fn get_shards(&self) -> ShardList {
        let shards = self.shards.par_iter().map(|(shard_id, shard)| ShardPB {
            shard_id: shard_id.to_string(),
            resources: shard.get_resources() as u64,
            paragraphs: 0_u64,
            sentences: 0_u64,
        });
        ShardList {
            shards: POOL.install(|| shards.collect()),
        }
    }
    pub fn suggest(
        &mut self,
        shard_id: &ShardId,
        request: SuggestRequest,
    ) -> Option<ServiceResult<SuggestResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.suggest(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    pub fn search(
        &mut self,
        shard_id: &ShardId,
        request: SearchRequest,
    ) -> Option<ServiceResult<SearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }

    pub fn relation_search(
        &mut self,
        shard_id: &ShardId,
        request: RelationSearchRequest,
    ) -> Option<ServiceResult<RelationSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.relation_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }

    pub fn vector_search(
        &mut self,
        shard_id: &ShardId,
        request: VectorSearchRequest,
    ) -> Option<ServiceResult<VectorSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.vector_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }

    pub fn paragraph_search(
        &mut self,
        shard_id: &ShardId,
        request: ParagraphSearchRequest,
    ) -> Option<ServiceResult<ParagraphSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.paragraph_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }

    pub fn document_search(
        &mut self,
        shard_id: &ShardId,
        request: DocumentSearchRequest,
    ) -> Option<ServiceResult<DocumentSearchResponse>> {
        self.get_shard(shard_id)
            .map(|shard| POOL.install(|| shard.document_search(request)))
            .map(|r| r.map_err(|e| e.into()))
    }
    pub fn document_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id) {
            let ids = POOL.install(|| shard.get_field_keys());
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub fn paragraph_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id) {
            let ids = POOL.install(|| shard.get_paragraphs_keys());
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub fn vector_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id) {
            let ids = POOL.install(|| shard.get_vectors_keys());
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub fn relation_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id) {
            let ids = POOL.install(|| shard.get_relations_keys());
            Some(IdCollection { ids })
        } else {
            None
        }
    }

    pub fn relation_edges(&mut self, shard_id: &ShardId) -> Option<EdgeList> {
        if let Some(shard) = self.get_shard(shard_id) {
            let edges = POOL.install(|| shard.get_relations_edges());
            Some(edges)
        } else {
            None
        }
    }

    pub fn relation_types(&mut self, shard_id: &ShardId) -> Option<TypeList> {
        if let Some(shard) = self.get_shard(shard_id) {
            let types = POOL.install(|| shard.get_relations_types());
            Some(types)
        } else {
            None
        }
    }
}
