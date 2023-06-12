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

use async_std::sync::RwLock;
use nucliadb_core::env;
use nucliadb_core::prelude::{DocumentIterator, ParagraphIterator};
use nucliadb_core::protos::node_reader_server::NodeReader;
use nucliadb_core::protos::*;
use nucliadb_core::tracing::{self, *};
use Shard as ShardPB;

use crate::reader::NodeReaderService;
use crate::shards::{ReaderShardsProvider, ShardReader, UnboundedShardReaderCache};
use crate::utils::nonblocking;

pub struct NodeReaderGRPCDriver {
    lazy_loading: bool,
    inner: RwLock<NodeReaderService>,
    shards: UnboundedShardReaderCache,
}

impl From<NodeReaderService> for NodeReaderGRPCDriver {
    fn from(node: NodeReaderService) -> NodeReaderGRPCDriver {
        NodeReaderGRPCDriver {
            lazy_loading: env::lazy_loading(),
            inner: RwLock::new(node),
            shards: UnboundedShardReaderCache::new(),
        }
    }
}

impl NodeReaderGRPCDriver {
    // The GRPC reader will only request the reader to bring a shard
    // to memory if lazy loading is enabled. Otherwise all the
    // shards on disk would have been brought to memory before the driver is online.
    #[tracing::instrument(skip_all)]
    async fn shard_loading(&self, id: &ShardId) {
        if self.lazy_loading {
            let mut writer = self.inner.write().await;
            writer.load_shard(id);

            let loaded = self.shards.load(id.id.clone()).await;
            if let Err(error) = loaded {
                // REVIEW if shard can't be loaded, why aren't we returning
                // an error?
                error!("Error lazy loading shard: {error:?}");
            }
        }
    }

    async fn obtain_shard(&self, id: impl Into<String>) -> Result<Arc<ShardReader>, tonic::Status> {
        let id = id.into();
        self.shard_loading(&ShardId { id: id.clone() }).await;

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

    // TODO
    async fn paragraphs(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::ParagraphsStream>, tonic::Status> {
        debug!("Starting paragraph streaming");
        let request = request.into_inner();
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(tonic::Status::not_found("Shard ID not present"));
        };

        let shard_id = ShardId { id: shard_id.id };
        self.shard_loading(&shard_id).await;

        let reader = self.inner.read().await;
        match reader.paragraph_iterator(&shard_id, request).transpose() {
            Some(Ok(response)) => {
                debug!("Stream created correctly");
                Ok(tonic::Response::new(GrpcStreaming(response)))
            }
            Some(Err(e)) => {
                debug!("Stream could not be created");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    // TODO
    async fn documents(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::DocumentsStream>, tonic::Status> {
        debug!("Starting document streaming");
        let request = request.into_inner();
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(tonic::Status::not_found("Shard ID not present"));
        };

        let shard_id = ShardId { id: shard_id.id };
        self.shard_loading(&shard_id).await;

        let reader = self.inner.read().await;
        match reader.document_iterator(&shard_id, request).transpose() {
            Some(Ok(response)) => {
                debug!("Document stream created correctly");
                Ok(tonic::Response::new(GrpcStreaming(response)))
            }
            Some(Err(e)) => {
                debug!("Document stream could not be created");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
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
        let response = nonblocking!({ shard.search(search_request) });
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
        let response = nonblocking!({ shard.suggest(suggest_request) });
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
        let response = nonblocking!({ shard.vector_search(vector_request) });
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
        let response = nonblocking!({ shard.relation_search(relation_request) });
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
        let response = nonblocking!({ shard.document_search(document_request) });
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
        let response = nonblocking!({ shard.paragraph_search(paragraph_request) });
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    // TODO
    async fn document_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.document_ids(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::internal(e.to_string())),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    // TODO
    async fn paragraph_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.paragraph_ids(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::internal(e.to_string())),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    // TODO
    async fn vector_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.vector_ids(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::internal(e.to_string())),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    // TODO
    async fn relation_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.relation_ids(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::internal(e.to_string())),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    // TODO
    async fn relation_edges(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EdgeList>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.relation_edges(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::not_found(format!("{e:?} in {shard_id:?}",))),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    // TODO
    async fn relation_types(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<TypeList>, tonic::Status> {
        debug!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.inner.read().await;
        match reader.relation_types(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::not_found(format!("{e:?} in {shard_id:?}",))),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {shard_id:?}"
            ))),
        }
    }
}
