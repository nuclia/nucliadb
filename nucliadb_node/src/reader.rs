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
use std::collections::HashMap;
use std::path::Path;

use async_std::sync::{Arc, RwLock};
use nucliadb_protos::node_reader_server::NodeReader;
use nucliadb_protos::{
    DocumentSearchRequest, DocumentSearchResponse, EmptyQuery, ParagraphSearchRequest,
    ParagraphSearchResponse, RelationSearchRequest, RelationSearchResponse, SearchRequest,
    SearchResponse, Shard as ShardPB, ShardId, ShardList, VectorSearchRequest,
    VectorSearchResponse,
};
use opentelemetry::global;
use tracing::{instrument, Span, *};
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::Configuration;
use crate::services::reader::ShardReaderService;
use crate::utils::MetadataMap;
pub type ShardReaderDB = Arc<RwLock<HashMap<String, ShardReaderService>>>;

#[derive(Debug)]
pub struct NodeReaderService {
    pub shards: ShardReaderDB,
}

impl NodeReaderService {
    pub fn new() -> NodeReaderService {
        NodeReaderService {
            shards: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Stop all shards on memory
    pub async fn shutdown(&self) {
        for (shard_id, shard) in self.shards.write().await.iter_mut() {
            info!("Stopping shard {}", shard_id);
            ShardReaderService::stop(shard).await;
        }
    }

    /// Load all shards on the shards memory structure
    pub async fn load_shards(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());

            let shard: ShardReaderService = ShardReaderService::start(&shard_id.to_string())
                .await
                .map_err(|e| e.as_tonic_status())?;
            self.shards.write().await.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    async fn load_shard(&self, shard_id: &str) {
        let in_memory = self.shards.read().await.contains_key(shard_id);
        if !in_memory {
            let in_disk = Path::new(&Configuration::shards_path_id(shard_id)).exists();
            if in_disk {
                let shard = ShardReaderService::start(shard_id).await.unwrap();
                info!("Loaded shard {:?}", shard_id);
                self.shards
                    .write()
                    .await
                    .insert(shard_id.to_string(), shard);
                info!("Inserted on memory {:?}", shard_id);
            }
        }
    }
}

impl Default for NodeReaderService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl NodeReader for NodeReaderService {
    #[instrument(skip(self, request))]
    async fn get_shard(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<ShardPB>, tonic::Status> {
        info!("Get shard start {}:{}", file!(), line!());
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let shard_request = request.into_inner();
        let shard_id = shard_request.id.as_str();
        self.load_shard(shard_id).await;
        info!("Loaded lets get {:?}", shard_id);
        match self.shards.read().await.get(shard_id) {
            Some(shard) => {
                info!("Ready {:?}", shard_id);
                let stats = shard.get_info().await;
                let result_shard = ShardPB {
                    shard_id: String::from(&shard.id),
                    resources: stats.resources as u64,
                    paragraphs: stats.paragraphs as u64,
                    sentences: stats.sentences as u64,
                };
                info!("Get shard ends {}:{}", file!(), line!());
                Ok(tonic::Response::new(result_shard))
            }
            None => {
                let message = format!("Shard not found {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn get_shards(
        &self,
        request: tonic::Request<EmptyQuery>,
    ) -> Result<tonic::Response<ShardList>, tonic::Status> {
        info!("Get shards starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let shards: Vec<ShardPB> = self
            .shards
            .read()
            .await
            .iter()
            .map(|(shard_id, shard)| {
                let resources = shard.get_resources();
                ShardPB {
                    shard_id: shard_id.to_string(),
                    resources: resources as u64,
                    paragraphs: 0_u64,
                    sentences: 0_u64,
                }
            })
            .collect();
        info!("Get shards ends");
        Ok(tonic::Response::new(ShardList { shards }))
    }

    async fn vector_search(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<VectorSearchResponse>, tonic::Status> {
        info!("Vector search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let vector_search = request.into_inner();
        let shard_id = &vector_search.id;
        self.load_shard(shard_id).await;

        match self.shards.read().await.get(shard_id) {
            Some(shard) => match shard.vector_search(vector_search).await {
                Ok(response) => {
                    info!("Vector search ended correctly");
                    Ok(tonic::Response::new(response))
                }
                Err(e) => {
                    info!("Vector search ended incorrectly");
                    Err(e.as_tonic_status())
                }
            },
            None => {
                let message = format!("Error loading shard {}", shard_id);
                Err(tonic::Status::internal(message))
            }
        }
    }

    async fn relation_search(
        &self,
        request: tonic::Request<RelationSearchRequest>,
    ) -> Result<tonic::Response<RelationSearchResponse>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let _relation_search = request.into_inner();
        todo!()
    }

    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        info!("Search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let search = request.into_inner();
        let shard_id = &search.shard;
        self.load_shard(shard_id).await;

        match self.shards.read().await.get(shard_id) {
            Some(shard) => match shard.search(search).await {
                Ok(response) => {
                    info!("Searh ended correctly");
                    Ok(tonic::Response::new(response))
                }
                Err(e) => {
                    info!("Search ended incorrectly");
                    Err(e.as_tonic_status())
                }
            },
            None => {
                let message = format!("Error loading shard {}", shard_id);
                Err(tonic::Status::internal(message))
            }
        }
    }

    async fn document_search(
        &self,
        request: tonic::Request<DocumentSearchRequest>,
    ) -> Result<tonic::Response<DocumentSearchResponse>, tonic::Status> {
        info!("Document search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let search_request = request.into_inner();

        let shard_id = &search_request.id;
        self.load_shard(shard_id).await;

        match self.shards.read().await.get(shard_id) {
            Some(shard) => match shard.document_search(search_request).await {
                Ok(response) => {
                    info!("Document search ended correctly");
                    Ok(tonic::Response::new(response))
                }
                Err(e) => {
                    info!("Document search ended incorrectly");
                    Err(e.as_tonic_status())
                }
            },
            None => {
                let message = format!("Error loading shard {}", shard_id);
                Err(tonic::Status::internal(message))
            }
        }
    }

    async fn paragraph_search(
        &self,
        request: tonic::Request<ParagraphSearchRequest>,
    ) -> Result<tonic::Response<ParagraphSearchResponse>, tonic::Status> {
        info!("Paragraph search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let search_request = request.into_inner();
        let shard_id = &search_request.id;
        self.load_shard(shard_id).await;

        match self.shards.read().await.get(shard_id) {
            Some(shard) => match shard.paragraph_search(search_request).await {
                Ok(response) => {
                    info!("Paragraph search ended correctly");
                    Ok(tonic::Response::new(response))
                }
                Err(e) => {
                    info!("Paragraph search ended incorrectly");
                    Err(e.as_tonic_status())
                }
            },
            None => {
                let message = format!("This shard doesn't exists: {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }
}
