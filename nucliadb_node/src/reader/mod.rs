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
    DocumentSearchRequest, DocumentSearchResponse, EdgeList, IdCollection, ParagraphSearchRequest,
    ParagraphSearchResponse, RelationSearchRequest, RelationSearchResponse, SearchRequest,
    SearchResponse, Shard as ShardPB, ShardId, ShardList, StreamRequest, SuggestRequest,
    SuggestResponse, TypeList, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_core::thread::*;
use nucliadb_core::tracing::{self, *};

use crate::env;
use crate::services::reader::ShardReaderService;

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

    #[tracing::instrument(skip_all)]
    pub fn iter_shards(
        &mut self,
    ) -> NodeResult<impl Iterator<Item = NodeResult<ShardReaderService>>> {
        let shards_path = env::shards_path();
        Ok(std::fs::read_dir(shards_path)?.flatten().map(|entry| {
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            info!("Opening {shard_path:?}");
            ShardReaderService::new(file_name, &shard_path)
        }))
    }

    /// Load all shards on the shards memory structure
    #[tracing::instrument(skip_all)]
    pub fn load_shards(&mut self) -> NodeResult<()> {
        let shards_path = env::shards_path();
        info!("Recovering shards from {shards_path:?}...");
        for entry in std::fs::read_dir(&shards_path)? {
            let entry = entry?;
            let file_name = entry.file_name().to_str().unwrap().to_string();
            let shard_path = entry.path();
            match ShardReaderService::new(file_name.clone(), &shard_path) {
                Err(err) => error!("Loading {shard_path:?} raised {err}"),
                Ok(shard) => {
                    info!("Shard loaded: {shard_path:?}");
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
            info!("Shard {shard_path:?} is already on memory");
            return;
        }
        if !shard_path.is_dir() {
            error!("Shard {shard_path:?} is not on disk");
            return;
        }
        let Ok(shard) = ShardReaderService::new(shard_name, &shard_path) else {
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
    pub fn get_shards(&self) -> NodeResult<ShardList> {
        use crate::telemetry::run_with_telemetry;
        let span = tracing::Span::current();
        let task = move |shard_id: &str, shard: &ShardReaderService| {
            run_with_telemetry(info_span!(parent: &span, "get shards"), || {
                shard.get_resources().map(|count| ShardPB {
                    shard_id: shard_id.to_string(),
                    resources: count as u64,
                    paragraphs: 0_u64,
                    sentences: 0_u64,
                })
            })
        };
        let shards = self
            .cache
            .par_iter()
            .map(|(id, shard)| task(id, shard))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(ShardList { shards })
    }
    #[tracing::instrument(skip_all)]
    pub fn suggest(
        &self,
        shard_id: &ShardId,
        request: SuggestRequest,
    ) -> NodeResult<Option<SuggestResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let suggest_response = shard.suggest(request)?;
        Ok(Some(suggest_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn search(
        &self,
        shard_id: &ShardId,
        request: SearchRequest,
    ) -> NodeResult<Option<SearchResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let search_response = shard.search(request)?;
        Ok(Some(search_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_search(
        &self,
        shard_id: &ShardId,
        request: RelationSearchRequest,
    ) -> NodeResult<Option<RelationSearchResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let search_response = shard.relation_search(request)?;
        Ok(Some(search_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_search(
        &self,
        shard_id: &ShardId,
        request: VectorSearchRequest,
    ) -> NodeResult<Option<VectorSearchResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let search_response = shard.vector_search(request)?;
        Ok(Some(search_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(
        &self,
        shard_id: &ShardId,
        request: ParagraphSearchRequest,
    ) -> NodeResult<Option<ParagraphSearchResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let search_response = shard.paragraph_search(request)?;
        Ok(Some(search_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_iterator(
        &self,
        shard_id: &ShardId,
        request: StreamRequest,
    ) -> NodeResult<Option<ParagraphIterator>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.paragraph_iterator(request)?))
    }
    #[tracing::instrument(skip_all)]
    pub fn document_iterator(
        &self,
        shard_id: &ShardId,
        request: StreamRequest,
    ) -> NodeResult<Option<DocumentIterator>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.document_iterator(request)?))
    }
    #[tracing::instrument(skip_all)]
    pub fn document_search(
        &self,
        shard_id: &ShardId,
        request: DocumentSearchRequest,
    ) -> NodeResult<Option<DocumentSearchResponse>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        let search_response = shard.document_search(request)?;
        Ok(Some(search_response))
    }
    #[tracing::instrument(skip_all)]
    pub fn document_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        let Some(shard) = self.get_shard(shard_id) else {
            return None;
        };
        let ids = shard.get_text_keys();
        Some(IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        let Some(shard) = self.get_shard(shard_id) else {
            return None;
        };
        let ids = shard.get_paragraphs_keys();
        Some(IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        let Some(shard) = self.get_shard(shard_id) else {
            return None;
        };
        let ids = shard.get_vectors_keys();
        Some(IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_ids(&self, shard_id: &ShardId) -> Option<IdCollection> {
        let Some(shard) = self.get_shard(shard_id) else {
            return None;
        };
        let ids = shard.get_relations_keys();
        Some(IdCollection { ids })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_edges(&self, shard_id: &ShardId) -> NodeResult<Option<EdgeList>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.get_relations_edges()?))
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_types(&self, shard_id: &ShardId) -> NodeResult<Option<TypeList>> {
        let Some(shard) = self.get_shard(shard_id) else {
            return Ok(None);
        };
        Ok(Some(shard.get_relations_types()?))
    }
}
