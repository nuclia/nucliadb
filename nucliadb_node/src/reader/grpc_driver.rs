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

use std::sync::Arc;

use nucliadb_core::prelude::{DocumentIterator, ParagraphIterator};
use nucliadb_core::protos::node_reader_server::NodeReader;
use nucliadb_core::protos::*;
use nucliadb_core::tracing::*;
use nucliadb_core::NodeResult;
use Shard as ShardPB;

use crate::shards::{AsyncReaderShardsProvider, AsyncUnboundedShardReaderCache, ShardReader};

pub struct NodeReaderGRPCDriver {
    shards: AsyncUnboundedShardReaderCache,
    options: GrpcReaderOptions,
}

pub struct GrpcReaderOptions {
    pub lazy_loading: bool,
}

impl NodeReaderGRPCDriver {
    pub fn new(options: GrpcReaderOptions) -> Self {
        Self {
            shards: AsyncUnboundedShardReaderCache::new(),
            options,
        }
    }

    /// This function must be called before using this service
    pub async fn initialize(&self) -> NodeResult<()> {
        if !self.options.lazy_loading {
            // If lazy loading is disabled, load
            self.shards.load_all().await?
        }
        Ok(())
    }

    async fn obtain_shard(&self, id: impl Into<String>) -> Result<Arc<ShardReader>, tonic::Status> {
        let id = id.into();

        // NOTE: Taking into account we use an unbounded shard cache, we only request
        // loading a shard into memory when lazy loading is enabled. Otherwise,
        // we rely on all shards (stored on disk) been brought to memory before
        // the driver is online.
        if self.options.lazy_loading {
            let loaded = self.shards.load(id.clone()).await;
            if let Err(error) = loaded {
                // REVIEW if shard can't be loaded, why aren't we returning
                // an error?
                error!("Error lazy loading shard: {error:?}");
            }
        }

        match self.shards.get(id.clone()).await {
            Some(shard) => Ok(shard),
            None => Err(tonic::Status::not_found(format!(
                "Error loading shard {id}: shard not found"
            ))),
        }
    }
}

pub struct GrpcStreaming<T>(T);

impl futures_core::Stream for GrpcStreaming<ParagraphIterator> {
    type Item = Result<ParagraphItem, tonic::Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.0.next().map(Ok))
    }
}

impl futures_core::Stream for GrpcStreaming<DocumentIterator> {
    type Item = Result<DocumentItem, tonic::Status>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::task::Poll::Ready(self.0.next().map(Ok))
    }
}

#[tonic::async_trait]
impl NodeReader for NodeReaderGRPCDriver {
    type ParagraphsStream = GrpcStreaming<ParagraphIterator>;
    type DocumentsStream = GrpcStreaming<DocumentIterator>;

    async fn paragraphs(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::ParagraphsStream>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let shard = self.obtain_shard(shard_id).await?;
        match shard.paragraph_iterator(request) {
            Ok(iterator) => Ok(tonic::Response::new(GrpcStreaming(iterator))),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn documents(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::DocumentsStream>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let shard = self.obtain_shard(shard_id).await?;
        match shard.document_iterator(request) {
            Ok(iterator) => Ok(tonic::Response::new(GrpcStreaming(iterator))),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn get_shard(
        &self,
        request: tonic::Request<GetShardRequest>,
    ) -> Result<tonic::Response<ShardPB>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let shard = self.obtain_shard(shard_id).await?;
        match shard.get_info(&request) {
            Ok(shard) => Ok(tonic::Response::new(shard)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let search_request = request.into_inner();
        let shard_id = search_request.shard.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response = tokio::task::spawn_blocking(move || shard.search(search_request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn suggest(
        &self,
        request: tonic::Request<SuggestRequest>,
    ) -> Result<tonic::Response<SuggestResponse>, tonic::Status> {
        let suggest_request = request.into_inner();
        let shard_id = suggest_request.shard.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response = tokio::task::spawn_blocking(move || shard.suggest(suggest_request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn vector_search(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<VectorSearchResponse>, tonic::Status> {
        let vector_request = request.into_inner();
        let shard_id = vector_request.id.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response = tokio::task::spawn_blocking(move || shard.vector_search(vector_request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_search(
        &self,
        request: tonic::Request<RelationSearchRequest>,
    ) -> Result<tonic::Response<RelationSearchResponse>, tonic::Status> {
        let relation_request = request.into_inner();
        let shard_id = relation_request.shard_id.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response = tokio::task::spawn_blocking(move || shard.relation_search(relation_request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn document_search(
        &self,
        request: tonic::Request<DocumentSearchRequest>,
    ) -> Result<tonic::Response<DocumentSearchResponse>, tonic::Status> {
        let document_request = request.into_inner();
        let shard_id = document_request.id.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response = tokio::task::spawn_blocking(move || shard.document_search(document_request))
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn paragraph_search(
        &self,
        request: tonic::Request<ParagraphSearchRequest>,
    ) -> Result<tonic::Response<ParagraphSearchResponse>, tonic::Status> {
        let paragraph_request = request.into_inner();
        let shard_id = paragraph_request.id.clone();
        let shard = self.obtain_shard(shard_id).await?;
        let response =
            tokio::task::spawn_blocking(move || shard.paragraph_search(paragraph_request))
                .await
                .map_err(|error| {
                    tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
                })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn document_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || {
            shard.get_text_keys().map(|ids| IdCollection { ids })
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn paragraph_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || {
            shard.get_paragraphs_keys().map(|ids| IdCollection { ids })
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn vector_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || {
            shard.get_vectors_keys().map(|ids| IdCollection { ids })
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || {
            shard.get_relations_keys().map(|ids| IdCollection { ids })
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_edges(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EdgeList>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || shard.get_relations_edges())
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_types(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<TypeList>, tonic::Status> {
        let shard_id = request.into_inner().id;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        let response = tokio::task::spawn_blocking(move || shard.get_relations_types())
            .await
            .map_err(|error| {
                tonic::Status::internal(format!("Blocking task panicked: {error:?}"))
            })?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }
}
