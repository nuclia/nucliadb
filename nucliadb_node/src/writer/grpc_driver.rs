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
use nucliadb_protos::node_writer_server::NodeWriter;
use nucliadb_protos::{
    op_status, DeleteGraphNodes, EmptyQuery, EmptyResponse, OpStatus, Resource, ResourceId,
    SetGraph, ShardCleaned, ShardCreated, ShardId, ShardIds, VectorSetId, VectorSetList,
};
use opentelemetry::global;
use tonic::{Request, Response, Status};
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;

use crate::config::Configuration;
use crate::utils::MetadataMap;
use crate::writer::NodeWriterService;

pub struct NodeWriterGRPCDriver(RwLock<NodeWriterService>);
impl From<NodeWriterService> for NodeWriterGRPCDriver {
    fn from(node: NodeWriterService) -> NodeWriterGRPCDriver {
        NodeWriterGRPCDriver(RwLock::new(node))
    }
}
impl NodeWriterGRPCDriver {
    // The GRPC writer will only request the writer to bring a shard
    // to memory if lazy loading is enabled. Otherwise all the
    // shards on disk would have been brought to memory before the driver is online.
    async fn shard_loading(&self, id: &ShardId) {
        if Configuration::lazy_loading() {
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
#[tonic::async_trait]
impl NodeWriter for NodeWriterGRPCDriver {
    #[tracing::instrument(name = "NodeWriterGRPCDriver::get_shard", skip(self, request))]
    async fn get_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>, Status> {
        self.instrument(&request);

        info!("{:?}: gRPC get_shard", request);
        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let reader = self.0.read().await;
        let result = reader.get_shard(&shard_id).is_some();
        std::mem::drop(reader);
        match result {
            true => {
                info!("{:?}: Ready readed", shard_id);
                Ok(tonic::Response::new(shard_id))
            }
            false => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(name = "NodeWriterGRPCDriver::new_shard", skip(self, request))]
    async fn new_shard(
        &self,
        request: Request<EmptyQuery>,
    ) -> Result<Response<ShardCreated>, Status> {
        self.instrument(&request);

        info!("Creating new shard");
        let mut writer = self.0.write().await;
        let result = writer.new_shard();
        std::mem::drop(writer);
        Ok(tonic::Response::new(result))
    }

    #[tracing::instrument(name = "NodeWriterGRPCDriver::delete_shard", skip(self, request))]
    async fn delete_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>, Status> {
        self.instrument(&request);

        info!("gRPC delete_shard {:?}", request);

        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let mut writer = self.0.write().await;
        let result = writer.delete_shard(&shard_id);
        std::mem::drop(writer);
        match result {
            Some(Ok(_)) => Ok(tonic::Response::new(shard_id)),
            Some(Err(e)) => {
                let error_msg = format!("Error deleting shard {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(
        name = "NodeWriterGRPCDriver::clean_and_upgrade_shard",
        skip(self, request)
    )]
    async fn clean_and_upgrade_shard(
        &self,
        request: Request<ShardId>,
    ) -> Result<Response<ShardCleaned>, Status> {
        self.instrument(&request);

        info!("gRPC delete_shard {:?}", request);

        let shard_id = request.into_inner();
        self.shard_loading(&shard_id).await;
        let mut writer = self.0.write().await;
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
        request: Request<EmptyQuery>,
    ) -> Result<Response<ShardIds>, Status> {
        self.instrument(&request);
        let ids = self.0.read().await.get_shard_ids();
        Ok(tonic::Response::new(ids))
    }

    // Incremental call that can be call multiple times for the same resource
    #[tracing::instrument(name = "NodeWriterGRPCDriver::set_resource", skip(self, request))]
    async fn set_resource(&self, request: Request<Resource>) -> Result<Response<OpStatus>, Status> {
        self.instrument(&request);
        let resource = request.into_inner();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };
        self.shard_loading(&shard_id).await;
        let mut writer = self.0.write().await;
        let result = writer.set_resource(&shard_id, &resource);
        std::mem::drop(writer);
        match result {
            Some(Ok(count)) => {
                info!("Set resource ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
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
                };
                Ok(tonic::Response::new(op_status))
            }
            None => {
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(
        name = "NodeWriterGRPCDriver::delete_relation_nodes",
        skip(self, request)
    )]
    async fn delete_relation_nodes(
        &self,
        request: Request<DeleteGraphNodes>,
    ) -> Result<Response<OpStatus>, Status> {
        self.instrument(&request);
        let request = request.into_inner();
        let shard_id = request.shard_id.as_ref().unwrap();
        let mut writer = self.0.write().await;
        match writer.delete_relation_nodes(shard_id, &request) {
            Some(Ok(count)) => {
                info!("Remove resource ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error joining graph {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    #[tracing::instrument(name = "NodeWriterGRPCDriver::join_graph", skip(self, request))]
    async fn join_graph(&self, request: Request<SetGraph>) -> Result<Response<OpStatus>, Status> {
        self.instrument(&request);
        let request = request.into_inner();
        let shard_id = request.shard_id.unwrap();
        let graph = request.graph.unwrap();
        let mut writer = self.0.write().await;
        match writer.join_relations_graph(&shard_id, &graph) {
            Some(Ok(count)) => {
                info!("Remove resource ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error joining graph {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }
    #[tracing::instrument(name = "NodeWriterGRPCDriver::remove_resource", skip(self, request))]
    async fn remove_resource(
        &self,
        request: Request<ResourceId>,
    ) -> Result<Response<OpStatus>, Status> {
        self.instrument(&request);
        let resource = request.into_inner();
        let shard_id = ShardId {
            id: resource.shard_id.clone(),
        };

        self.shard_loading(&shard_id).await;
        let mut writer = self.0.write().await;
        let result = writer.remove_resource(&shard_id, &resource);
        std::mem::drop(writer);

        match result {
            Some(Ok(count)) => {
                info!("Remove resource ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
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
        request: Request<VectorSetId>,
    ) -> Result<Response<OpStatus>, Status> {
        self.instrument(&request);
        let request = request.into_inner();
        let shard_id = request.shard.as_ref().unwrap();
        let mut writer = self.0.write().await;
        match writer.add_vectorset(shard_id, &request) {
            Some(Ok(count)) => {
                info!("add_vector_set ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error adding vector set {:?}: {}", shard_id, e);
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
        self.instrument(&request);
        let request = request.into_inner();
        let shard_id = request.shard.as_ref().unwrap();
        let mut writer = self.0.write().await;
        match writer.remove_vectorset(shard_id, &request) {
            Some(Ok(count)) => {
                info!("remove_vector_set ends correctly");
                let status = OpStatus {
                    status: 0,
                    detail: "Success!".to_string(),
                    count: count as u64,
                    shard_id: shard_id.id.clone(),
                };
                Ok(tonic::Response::new(status))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error removing vector set {:?}: {}", shard_id, e);
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
        self.instrument(&request);
        let shard_id = request.into_inner();
        let reader = self.0.read().await;
        match reader.list_vectorsets(&shard_id) {
            Some(Ok(list)) => {
                info!("list_vectorset ends correctly");
                let list = VectorSetList {
                    shard: Some(shard_id),
                    vectorset: list,
                };
                Ok(tonic::Response::new(list))
            }
            Some(Err(e)) => {
                let error_msg = format!("Error listing sets {:?}: {}", shard_id, e);
                error!("{}", error_msg);
                Err(tonic::Status::internal(error_msg))
            }
            None => {
                let message = format!("Shard not found {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }
    #[tracing::instrument(name = "NodeWriterGRPCDriver::gc", skip(self, request))]
    async fn gc(&self, request: Request<ShardId>) -> Result<Response<EmptyResponse>, Status> {
        self.instrument(&request);
        let shard_id = request.into_inner();
        info!("Running garbage collection at {}", shard_id.id);
        self.shard_loading(&shard_id).await;
        let mut writer = self.0.write().await;
        let result = writer.gc(&shard_id);
        std::mem::drop(writer);
        match result {
            Some(Ok(_)) => {
                info!("Garbage collection at {} was successful", shard_id.id);
                let resp = EmptyResponse {};
                Ok(tonic::Response::new(resp))
            }
            Some(Err(_)) => {
                info!("Garbage collection at {} raised an error", shard_id.id);
                let resp = EmptyResponse {};
                Ok(tonic::Response::new(resp))
            }
            None => {
                info!("{} was not found", shard_id.id);
                let message = format!("Error loading shard {:?}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;

    use nucliadb_protos::node_writer_client::NodeWriterClient;
    use nucliadb_protos::node_writer_server::NodeWriterServer;
    use portpicker::pick_unused_port;
    use tonic::transport::Server;
    use tonic::Request;

    use super::*;
    use crate::config::Configuration;
    use crate::utils::socket_to_endpoint;

    async fn start_test_server(address: SocketAddr) -> anyhow::Result<()> {
        let node_writer = NodeWriterGRPCDriver::from(NodeWriterService::new());
        std::fs::create_dir_all(Configuration::shards_path())?;

        let _ = tokio::spawn(async move {
            let node_writer_server = NodeWriterServer::new(node_writer);
            Server::builder()
                .add_service(node_writer_server)
                .serve(address)
                .await?;
            Result::<_, anyhow::Error>::Ok(())
        });
        Ok(())
    }

    #[tokio::test]
    async fn test_new_and_get_shard() -> anyhow::Result<()> {
        let port: u16 = pick_unused_port().expect("No ports free");

        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        start_test_server(grpc_addr).await?;
        let mut client = NodeWriterClient::new(socket_to_endpoint(grpc_addr)?.connect_lazy());

        let response = client
            .new_shard(Request::new(EmptyQuery {}))
            .await
            .expect("Error in new_shard request");
        let shard_id = &response.get_ref().id;

        let response = client
            .get_shard(Request::new(ShardId {
                id: shard_id.clone(),
            }))
            .await
            .expect("Error in get_shard request");
        let response_id = &response.get_ref().id;

        assert_eq!(shard_id, response_id);

        Ok(())
    }

    #[tokio::test]
    async fn test_list_shards() -> anyhow::Result<()> {
        let port: u16 = pick_unused_port().expect("No ports free");
        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        start_test_server(grpc_addr).await?;
        let mut request_ids: Vec<String> = Vec::new();

        let mut client = NodeWriterClient::new(socket_to_endpoint(grpc_addr)?.connect_lazy());

        for _ in 1..10 {
            let response = client
                .new_shard(Request::new(EmptyQuery {}))
                .await
                .expect("Error in new_shard request");

            request_ids.push(response.get_ref().id.clone());
        }
        let response = client
            .list_shards(Request::new(EmptyQuery {}))
            .await
            .expect("Error in list_shards request");

        let response_ids: Vec<String> = response
            .get_ref()
            .ids
            .iter()
            .map(|s| s.id.clone())
            .collect();

        assert!(request_ids.iter().all(|item| response_ids.contains(item)));

        Ok(())
    }

    #[tokio::test]
    async fn test_delete_shards() -> anyhow::Result<()> {
        let port: u16 = pick_unused_port().expect("No ports free");
        let grpc_addr: SocketAddr = format!("127.0.0.1:{}", port).parse()?;
        start_test_server(grpc_addr).await?;
        let mut request_ids: Vec<String> = Vec::new();

        let mut client = NodeWriterClient::new(socket_to_endpoint(grpc_addr)?.connect_lazy());

        let response = client
            .list_shards(Request::new(EmptyQuery {}))
            .await
            .expect("Error in list_shards request");

        assert_eq!(response.get_ref().ids.len(), 0);

        for _ in 0..10 {
            let response = client
                .new_shard(Request::new(EmptyQuery {}))
                .await
                .expect("Error in new_shard request");

            request_ids.push(response.get_ref().id.clone());
        }

        for id in request_ids.iter().cloned() {
            _ = client
                .clean_and_upgrade_shard(Request::new(ShardId { id }))
                .await
                .expect("Error in new_shard request");
        }

        for (id, expected) in request_ids.iter().map(|v| (v.clone(), v.clone())) {
            let response = client
                .delete_shard(Request::new(ShardId { id }))
                .await
                .expect("Error in delete_shard request");
            let deleted_id = response.get_ref().id.clone();
            assert_eq!(deleted_id, expected);
        }

        let response = client
            .list_shards(Request::new(EmptyQuery {}))
            .await
            .expect("Error in list_shards request");

        assert_eq!(response.get_ref().ids.len(), 0);

        Ok(())
    }
}
