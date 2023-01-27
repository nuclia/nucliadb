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

use async_std::sync::RwLock;
use nucliadb_core::prelude::{DocumentIterator, ParagraphIterator};
use nucliadb_core::protos::node_reader_server::NodeReader;
use nucliadb_core::protos::*;
use nucliadb_core::tracing::{self, *};
use opentelemetry::global;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use Shard as ShardPB;

use crate::env;
use crate::reader::NodeReaderService;
use crate::utils::MetadataMap;

pub struct NodeReaderGRPCDriver(RwLock<NodeReaderService>);
impl From<NodeReaderService> for NodeReaderGRPCDriver {
    fn from(node: NodeReaderService) -> NodeReaderGRPCDriver {
        NodeReaderGRPCDriver(RwLock::new(node))
    }
}
impl NodeReaderGRPCDriver {
    // The GRPC reader will only request the reader to bring a shard
    // to memory if lazy loading is enabled. Otherwise all the
    // shards on disk would have been brought to memory before the driver is online.
    #[tracing::instrument(skip_all)]
    async fn shard_loading(&self, id: &ShardId) {
        if env::lazy_loading() {
            let mut writer = self.0.write().await;
            writer.load_shard(id);
        }
    }

    // Instrumentation utilities for telemetry
    fn instrument<T>(&self, request: &tonic::Request<T>) {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
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
        info!("Starting paragraph streaming");
        self.instrument(&request);
        let request = request.into_inner();
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(tonic::Status::not_found("Shard ID not present"));
        };
        let shard_id = ShardId { id: shard_id.id };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.paragraph_iterator(&shard_id, request).transpose() {
            Some(Ok(response)) => {
                info!("Stream created correctly");
                Ok(tonic::Response::new(GrpcStreaming(response)))
            }
            Some(Err(e)) => {
                info!("Stream could not be created");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn documents(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::DocumentsStream>, tonic::Status> {
        info!("Starting document streaming");
        self.instrument(&request);
        let request = request.into_inner();
        let Some(shard_id) = request.shard_id.clone() else {
            return Err(tonic::Status::not_found("Shard ID not present"));
        };
        let shard_id = ShardId { id: shard_id.id };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.document_iterator(&shard_id, request).transpose() {
            Some(Ok(response)) => {
                info!("Document stream created correctly");
                Ok(tonic::Response::new(GrpcStreaming(response)))
            }
            Some(Err(e)) => {
                info!("Document stream could not be created");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn get_shard(
        &self,
        request: tonic::Request<GetShardRequest>,
    ) -> Result<tonic::Response<ShardPB>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let request = request.into_inner();
        let shard_id = request.shard_id.as_ref().unwrap();
        self.shard_loading(shard_id).await;
        let reader = self.0.read().await;
        match reader.get_shard(shard_id).map(|s| s.get_info(&request)) {
            Some(Ok(stats)) => {
                info!("Ready {:?}", shard_id);
                let result_shard = ShardPB {
                    shard_id: shard_id.id.clone(),
                    resources: stats.resources as u64,
                    paragraphs: stats.paragraphs as u64,
                    sentences: stats.sentences as u64,
                };
                info!("Get shard ends {}:{}", file!(), line!());
                Ok(tonic::Response::new(result_shard))
            }
            Some(Err(e)) => {
                info!("get_shard ended incorrectly");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn get_shards(
        &self,
        request: tonic::Request<EmptyQuery>,
    ) -> Result<tonic::Response<ShardList>, tonic::Status> {
        info!("Get shards starts");
        self.instrument(&request);
        self.0
            .read()
            .await
            .get_shards()
            .map(tonic::Response::new)
            .map_err(|e| tonic::Status::internal(e.to_string()))
    }

    #[tracing::instrument(skip_all)]
    async fn vector_search(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<VectorSearchResponse>, tonic::Status> {
        info!("Vector search starts");
        self.instrument(&request);
        let vector_request = request.into_inner();
        let shard_id = ShardId {
            id: vector_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.vector_search(&shard_id, vector_request).transpose() {
            Some(Ok(response)) => {
                info!("Vector search ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Vector search ended incorrectly");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn relation_search(
        &self,
        request: tonic::Request<RelationSearchRequest>,
    ) -> Result<tonic::Response<RelationSearchResponse>, tonic::Status> {
        info!("Relation search starts");
        self.instrument(&request);
        let relation_request = request.into_inner();
        let shard_id = ShardId {
            id: relation_request.shard_id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader
            .relation_search(&shard_id, relation_request)
            .transpose()
        {
            Some(Ok(response)) => {
                info!("Relation search ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Relation search ended incorrectly");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        info!("Search starts");
        self.instrument(&request);
        let search_request = request.into_inner();
        let shard_id = ShardId {
            id: search_request.shard.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.search(&shard_id, search_request).transpose() {
            Some(Ok(response)) => {
                info!("Document search ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Document search ended incorrectly {:?}", e.to_string());
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn suggest(
        &self,
        request: tonic::Request<SuggestRequest>,
    ) -> Result<tonic::Response<SuggestResponse>, tonic::Status> {
        info!("Suggest starts");
        self.instrument(&request);
        let suggest_request = request.into_inner();
        let shard_id = ShardId {
            id: suggest_request.shard.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.suggest(&shard_id, suggest_request).transpose() {
            Some(Ok(response)) => {
                info!("Suggest ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Suggest ended incorrectly");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn document_search(
        &self,
        request: tonic::Request<DocumentSearchRequest>,
    ) -> Result<tonic::Response<DocumentSearchResponse>, tonic::Status> {
        info!("Document search starts");
        self.instrument(&request);

        let document_request = request.into_inner();
        let shard_id = ShardId {
            id: document_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader
            .document_search(&shard_id, document_request)
            .transpose()
        {
            Some(Ok(response)) => {
                info!("Document search ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Document search ended incorrectly {:?}", e.to_string());
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn paragraph_search(
        &self,
        request: tonic::Request<ParagraphSearchRequest>,
    ) -> Result<tonic::Response<ParagraphSearchResponse>, tonic::Status> {
        info!("Paragraph search starts");
        self.instrument(&request);
        let paragraph_request = request.into_inner();
        let shard_id = ShardId {
            id: paragraph_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader
            .paragraph_search(&shard_id, paragraph_request)
            .transpose()
        {
            Some(Ok(response)) => {
                info!("Paragraph search ended correctly");
                Ok(tonic::Response::new(response))
            }
            Some(Err(e)) => {
                info!("Paragraph search ended incorrectly");
                Err(tonic::Status::internal(e.to_string()))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(skip_all)]
    async fn document_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.document_ids(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn paragraph_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.paragraph_ids(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn vector_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.vector_ids(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }
    #[tracing::instrument(skip_all)]
    async fn relation_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_ids(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn relation_edges(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EdgeList>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_edges(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::not_found(format!("{e:?} in {shard_id:?}",))),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    #[tracing::instrument(skip_all)]
    async fn relation_types(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<TypeList>, tonic::Status> {
        self.instrument(&request);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_types(&shard_id).transpose() {
            Some(Ok(ids)) => Ok(tonic::Response::new(ids)),
            Some(Err(e)) => Err(tonic::Status::not_found(format!("{e:?} in {shard_id:?}",))),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {shard_id:?}"
            ))),
        }
    }
}
