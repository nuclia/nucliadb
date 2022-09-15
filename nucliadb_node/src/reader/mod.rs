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
use tracing::*;

use crate::config::Configuration;
use crate::services::reader::ShardReaderService;

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
    pub async fn shutdown(&mut self) {
        for (shard_id, shard) in &mut self.shards {
            info!("Stopping shard {}", shard_id);
            ShardReaderService::stop(shard).await;
        }
    }

    /// Load all shards on the shards memory structure
    pub async fn load_shards(&mut self) -> ServiceResult<()> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());

            let shard: ShardReaderService = ShardReaderService::open(&shard_id.to_string()).await?;
            self.shards.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    #[instrument(name = "NodeReaderService::load_shard", skip(self))]
    pub async fn load_shard(&mut self, shard_id: &ShardId) {
        let shard_id = &shard_id.id;
        info!("{}: Loading shard", shard_id);
        let in_memory = self.shards.contains_key(shard_id);
        if !in_memory {
            info!("{}: Shard was not in memory", shard_id);
            let in_disk = Path::new(&Configuration::shards_path_id(shard_id)).exists();
            if in_disk {
                info!("{}: Shard was in disk", shard_id);
                let shard = ShardReaderService::open(shard_id).await.unwrap();
                info!("{}: Loaded shard", shard_id);
                self.shards.insert(shard_id.to_string(), shard);
                info!("{}: Inserted on memory", shard_id);
            }
        } else {
            info!("{}: Shard was in memory", shard_id);
        }
    }
    pub async fn get_shard(&mut self, shard_id: &ShardId) -> Option<&ShardReaderService> {
        self.load_shard(shard_id).await;
        self.shards.get(&shard_id.id)
    }
    pub async fn get_mut_shard(&mut self, shard_id: &ShardId) -> Option<&mut ShardReaderService> {
        self.load_shard(shard_id).await;
        self.shards.get_mut(&shard_id.id)
    }
    pub async fn get_shards(&self) -> ShardList {
        let mut shards = Vec::with_capacity(self.shards.len());
        for (shard_id, shard) in &self.shards {
            let resources = shard.get_resources();
            let elem = ShardPB {
                shard_id: shard_id.to_string(),
                resources: resources as u64,
                paragraphs: 0_u64,
                sentences: 0_u64,
            };
            shards.push(elem);
        }
        ShardList { shards }
    }
    pub async fn suggest(
        &mut self,
        shard_id: &ShardId,
        request: SuggestRequest,
    ) -> Option<ServiceResult<SuggestResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.suggest(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }
    pub async fn search(
        &mut self,
        shard_id: &ShardId,
        request: SearchRequest,
    ) -> Option<ServiceResult<SearchResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.search(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }

    pub async fn relation_search(
        &mut self,
        shard_id: &ShardId,
        request: RelationSearchRequest,
    ) -> Option<ServiceResult<RelationSearchResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.relation_search(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }

    pub async fn vector_search(
        &mut self,
        shard_id: &ShardId,
        request: VectorSearchRequest,
    ) -> Option<ServiceResult<VectorSearchResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.vector_search(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }

    pub async fn paragraph_search(
        &mut self,
        shard_id: &ShardId,
        request: ParagraphSearchRequest,
    ) -> Option<ServiceResult<ParagraphSearchResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.paragraph_search(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }

    pub async fn document_search(
        &mut self,
        shard_id: &ShardId,
        request: DocumentSearchRequest,
    ) -> Option<ServiceResult<DocumentSearchResponse>> {
        if let Some(shard) = self.get_shard(shard_id).await {
            Some(shard.document_search(request).await.map_err(|e| e.into()))
        } else {
            None
        }
    }
    pub async fn document_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let ids = shard.get_field_keys().await;
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub async fn paragraph_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let ids = shard.get_paragraphs_keys().await;
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub async fn vector_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let ids = shard.get_vectors_keys().await;
            Some(IdCollection { ids })
        } else {
            None
        }
    }
    pub async fn relation_ids(&mut self, shard_id: &ShardId) -> Option<IdCollection> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let ids = shard.get_relations_keys().await;
            Some(IdCollection { ids })
        } else {
            None
        }
    }

    pub async fn relation_edges(&mut self, shard_id: &ShardId) -> Option<EdgeList> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let edges = shard.get_relations_edges().await;
            Some(edges)
        } else {
            None
        }
    }

    pub async fn relation_types(&mut self, shard_id: &ShardId) -> Option<TypeList> {
        if let Some(shard) = self.get_shard(shard_id).await {
            let types = shard.get_relations_types().await;
            Some(types)
        } else {
            None
        }
    }
}
