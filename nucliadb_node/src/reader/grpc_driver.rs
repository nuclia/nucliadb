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
use nucliadb_protos::node_reader_server::NodeReader;
use nucliadb_protos::*;
use opentelemetry::global;
use tracing::{instrument, Span, *};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use Shard as ShardPB;

use crate::config::Configuration;
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
    async fn shard_loading(&self, id: &ShardId) {
        if Configuration::lazy_loading() {
            let mut writer = self.0.write().await;
            writer.load_shard(id);
        }
    }
}
#[tonic::async_trait]
impl NodeReader for NodeReaderGRPCDriver {
    #[instrument(name = "NodeReaderGRPCDriver::get_shard", skip(self, request))]
    async fn get_shard(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<ShardPB>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        let shard = reader.get_shard(&shard_id);
        match shard {
            Some(shard) => {
                info!("Ready {:?}", shard_id);
                let stats = shard.get_info();
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
                let message = format!("Shard not found {:?}", shard_id);
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
        let shards = self.0.read().await.get_shards();
        info!("Get shards ends");
        Ok(tonic::Response::new(shards))
    }

    #[tracing::instrument(name = "NodeReaderGRPCDriver::vector_search", skip(self, request))]
    async fn vector_search(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<VectorSearchResponse>, tonic::Status> {
        info!("Vector search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let vector_request = request.into_inner();
        let shard_id = ShardId {
            id: vector_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.vector_search(&shard_id, vector_request) {
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::relation_search", skip(self, request))]
    async fn relation_search(
        &self,
        request: tonic::Request<RelationSearchRequest>,
    ) -> Result<tonic::Response<RelationSearchResponse>, tonic::Status> {
        info!("Relation search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let relation_request = request.into_inner();
        let shard_id = ShardId {
            id: relation_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_search(&shard_id, relation_request) {
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::search", skip(self, request))]
    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        info!("Search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let search_request = request.into_inner();
        let shard_id = ShardId {
            id: search_request.shard.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.search(&shard_id, search_request) {
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

    #[instrument(name = "NodeReaderGRPCDriver::suggest", skip(self, request))]
    async fn suggest(
        &self,
        request: tonic::Request<SuggestRequest>,
    ) -> Result<tonic::Response<SuggestResponse>, tonic::Status> {
        info!("Suggest starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let suggest_request = request.into_inner();
        let shard_id = ShardId {
            id: suggest_request.shard.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.suggest(&shard_id, suggest_request) {
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::document_search", skip(self, request))]
    async fn document_search(
        &self,
        request: tonic::Request<DocumentSearchRequest>,
    ) -> Result<tonic::Response<DocumentSearchResponse>, tonic::Status> {
        info!("Document search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let document_request = request.into_inner();
        let shard_id = ShardId {
            id: document_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.document_search(&shard_id, document_request) {
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::paragraph_search", skip(self, request))]
    async fn paragraph_search(
        &self,
        request: tonic::Request<ParagraphSearchRequest>,
    ) -> Result<tonic::Response<ParagraphSearchResponse>, tonic::Status> {
        info!("Paragraph search starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let paragraph_request = request.into_inner();
        let shard_id = ShardId {
            id: paragraph_request.id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.paragraph_search(&shard_id, paragraph_request) {
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::document_ids", skip(self, request))]
    async fn document_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
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
    #[tracing::instrument(name = "NodeReaderGRPCDriver::paragraph_ids", skip(self, request))]
    async fn paragraph_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
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

    #[tracing::instrument(name = "NodeReaderGRPCDriver::vector_ids", skip(self, request))]
    async fn vector_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
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
    async fn relation_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
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

    async fn relation_edges(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EdgeList>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_edges(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }

    async fn relation_types(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<TypeList>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        match reader.relation_types(&shard_id) {
            Some(ids) => Ok(tonic::Response::new(ids)),
            None => Err(tonic::Status::not_found(format!(
                "Shard not found {:?}",
                shard_id
            ))),
        }
    }
}
