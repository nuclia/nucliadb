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
use nucliadb_core::protos::node_writer_server::NodeWriter;
use nucliadb_core::protos::{
    op_status, DeleteGraphNodes, EmptyQuery, EmptyResponse, NewShardRequest, NewVectorSetRequest,
    NodeMetadata, OpStatus, Resource, ResourceId, SetGraph, ShardCleaned, ShardCreated, ShardId,
    ShardIds, VectorSetId, VectorSetList,
};
use nucliadb_core::tracing::{self, *};
use nucliadb_telemetry::payload::TelemetryEvent;
use nucliadb_telemetry::sync::send_telemetry_event;
use tokio::sync::mpsc::UnboundedSender;
use tonic::{Request, Response, Status};

use crate::env;
use crate::writer::NodeWriterService;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeWriterEvent {
    ShardCreation,
    ShardDeletion,
}

pub struct NodeWriterGRPCDriver {
    lazy_loading: bool,
    inner: RwLock<NodeWriterService>,
    sender: Option<UnboundedSender<NodeWriterEvent>>,
}

impl From<NodeWriterService> for NodeWriterGRPCDriver {
    fn from(node: NodeWriterService) -> NodeWriterGRPCDriver {
        NodeWriterGRPCDriver {
            lazy_loading: env::lazy_loading(),
            inner: RwLock::new(node),
            sender: None,
        }
    }
}

impl NodeWriterGRPCDriver {
    pub fn with_sender(self, sender: UnboundedSender<NodeWriterEvent>) -> Self {
        Self {
            sender: Some(sender),
            ..self
        }
    }
    // The GRPC writer will only request the writer to bring a shard
    // to memory if lazy loading is enabled. Otherwise all the
    // shards on disk would have been brought to memory before the driver is online.
    #[tracing::instrument(skip_all)]
    async fn load_shard(&self, id: &ShardId) {
        if self.lazy_loading {
            let mut writer = self.inner.write().await;
            writer.load_shard(id);
        }
    }

    #[tracing::instrument(skip_all)]
    fn emit_event(&self, event: NodeWriterEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event);
        }
    }
}

#[tonic::async_trait]
impl NodeWriter for NodeWriterGRPCDriver {
    async fn new_shard(
        &self,
        request: Request<NewShardRequest>,
    ) -> Result<Response<ShardCreated>, Status> {
        debug!("Creating new shard");
        let request = request.into_inner();
        send_telemetry_event(TelemetryEvent::Create).await;
        let result = NodeWriterService::new_shard(&request)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        self.emit_event(NodeWriterEvent::ShardCreation);
        Ok(tonic::Response::new(result))
    }

    async fn delete_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>, Status> {
        debug!("gRPC delete_shard {:?}", request);
        send_telemetry_event(TelemetryEvent::Delete).await;
        // Deletion does not require for the shard
        // to be loaded.
        let shard_id = request.into_inner();
        let mut writer = self.inner.write().await;
        let result = writer.delete_shard(&shard_id);
        std::mem::drop(writer);
        match result {
            Ok(_) => {
                self.emit_event(NodeWriterEvent::ShardDeletion);
                Ok(tonic::Response::new(shard_id))
            }
            Err(e) => {
                let error_msg = format!("Error deleting shard {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
        }
    }

    async fn clean_and_upgrade_shard(
        &self,
        request: Request<ShardId>,
    ) -> Result<Response<ShardCleaned>, Status> {
        debug!("gRPC delete_shard {:?}", request);

        // Deletion and upgrade do not require for the shard
        // to be loaded.
        let shard_id = request.into_inner();
        let mut writer = self.inner.write().await;
        let result = writer.clean_and_upgrade_shard(&shard_id);
        std::mem::drop(writer);
        match result {
            Ok(updated) => Ok(tonic::Response::new(updated)),
            Err(e) => {
                let error_msg = format!("Error deleting shard {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
        }
    }

    async fn list_shards(
        &self,
        _request: Request<EmptyQuery>,
    ) -> Result<Response<ShardIds>, Status> {
        let ids = self.inner.read().await.get_shard_ids();
        Ok(tonic::Response::new(ids))
    }

    // Incremental call that can be call multiple times for the same resource
    async fn set_resource(&self, request: Request<Resource>) -> Result<Response<OpStatus>, Status> {
        let resource = request.into_inner();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };
        self.load_shard(&shard_id).await;

        let inner = self.inner.read().await;
        let result = inner.set_resource(&shard_id, &resource);
        match result.transpose() {
            Some(Ok(mut status)) => {
                debug!("Set resource ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let status = op_status::Status::Error as i32;
                let detail = format!("Error: {}", e);
                let op_status = OpStatus {
                    status,
                    detail,
                    count: 0_u64,
                    shard_id: shard_id.id.clone(),
                    ..Default::default()
                };
                Ok(tonic::Response::new(op_status))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn delete_relation_nodes(
        &self,
        request: Request<DeleteGraphNodes>,
    ) -> Result<Response<OpStatus>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id.as_ref().unwrap();
        self.load_shard(shard_id).await;

        let inner = self.inner.read().await;
        match inner.delete_relation_nodes(shard_id, &request).transpose() {
            Some(Ok(mut status)) => {
                debug!("Delete relations ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn join_graph(&self, request: Request<SetGraph>) -> Result<Response<OpStatus>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id.unwrap();
        let graph = request.graph.unwrap();
        self.load_shard(&shard_id).await;

        let inner = self.inner.read().await;
        match inner.join_relations_graph(&shard_id, &graph).transpose() {
            Some(Ok(mut status)) => {
                debug!("Join graph ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn remove_resource(
        &self,
        request: Request<ResourceId>,
    ) -> Result<Response<OpStatus>, Status> {
        let resource = request.into_inner();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };
        self.load_shard(&shard_id).await;

        let inner = self.inner.read().await;
        let result = inner.remove_resource(&shard_id, &resource);
        match result.transpose() {
            Some(Ok(mut status)) => {
                debug!("Remove resource ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();

                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let status = op_status::Status::Error as i32;
                let detail = format!("Error: {}", e);
                let op_status = OpStatus {
                    status,
                    detail,
                    count: 0_u64,
                    shard_id: shard_id.id.clone(),
                    ..Default::default()
                };
                Ok(tonic::Response::new(op_status))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn add_vector_set(
        &self,
        request: Request<NewVectorSetRequest>,
    ) -> Result<Response<OpStatus>, Status> {
        let request = request.into_inner();
        let shard_id = request.id.as_ref().and_then(|i| i.shard.clone()).unwrap();
        self.load_shard(&shard_id).await;

        let inner = self.inner.read().await;
        match inner.add_vectorset(&request).transpose() {
            Some(Ok(mut status)) => {
                debug!("add_vector_set ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn remove_vector_set(
        &self,
        request: Request<VectorSetId>,
    ) -> Result<Response<OpStatus>, Status> {
        let request = request.into_inner();
        let shard_id = request.shard.as_ref().unwrap();
        self.load_shard(shard_id).await;

        let inner = self.inner.read().await;
        match inner.remove_vectorset(shard_id, &request).transpose() {
            Some(Ok(mut status)) => {
                debug!("remove_vector_set ends correctly");
                status.status = 0;
                status.detail = "Success!".to_string();
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn list_vector_sets(
        &self,
        request: Request<ShardId>,
    ) -> Result<Response<VectorSetList>, Status> {
        let shard_id = request.into_inner();
        let reader = self.inner.read().await;
        match reader.list_vectorsets(&shard_id).transpose() {
            Some(Ok(list)) => {
                debug!("list_vectorset ends correctly");
                let list = VectorSetList {
                    shard: Some(shard_id),
                    vectorset: list,
                };
                Ok(tonic::Response::new(list))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn get_metadata(
        &self,
        _request: Request<EmptyQuery>,
    ) -> Result<Response<NodeMetadata>, Status> {
        match crate::node_metadata::NodeMetadata::load(&env::metadata_path()) {
            Ok(node_metadata) => Ok(tonic::Response::new(node_metadata.into())),
            Err(e) => {
                let e = format!("Cannot get node metadata: {e}");

                error!("{e}");

                Err(tonic::Status::internal(e))
            }
        }
    }

    async fn gc(&self, request: Request<ShardId>) -> Result<Response<EmptyResponse>, Status> {
        send_telemetry_event(TelemetryEvent::GarbageCollect).await;
        let shard_id = request.into_inner();
        debug!("Running garbage collection at {}", shard_id.id);
        self.load_shard(&shard_id).await;

        let inner = self.inner.read().await;
        let result = inner.gc(&shard_id);
        std::mem::drop(inner);
        match result.transpose() {
            Some(Ok(_)) => {
                debug!("Garbage collection at {} was successful", shard_id.id);
                let resp = EmptyResponse {};
                Ok(tonic::Response::new(resp))
            }
            Some(Err(_)) => {
                debug!("Garbage collection at {} raised an error", shard_id.id);
                let resp = EmptyResponse {};
                Ok(tonic::Response::new(resp))
            }
            None => {
                debug!("{} was not found", shard_id.id);
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }
}
