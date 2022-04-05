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
use nucliadb_protos::node_writer_server::NodeWriter;
use nucliadb_protos::{
    op_status, DelRelationsRequest, DelVectorFieldRequest, EmptyQuery, EmptyResponse, OpStatus,
    Resource, ResourceId, SetRelationsRequest, SetVectorFieldRequest, ShardCreated, ShardId,
    ShardIds,
};
use opentelemetry::global;
use tracing::*;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::config::Configuration;
use crate::services::writer::ShardWriterService;
use crate::utils::MetadataMap;

pub type ShardWriterDB = Arc<RwLock<HashMap<String, ShardWriterService>>>;

/// Stores the shards internal memory database
#[derive(Debug)]
pub struct NodeWriterService {
    /// The hashmap of shards on memory
    pub shards: ShardWriterDB,
}

impl NodeWriterService {
    pub fn new() -> NodeWriterService {
        NodeWriterService {
            shards: Arc::new(RwLock::new(HashMap::new())),
        }
    }
    /// Stop all shards on memory
    pub async fn shutdown(&self) {
        for (shard_id, shard) in self.shards.write().await.iter_mut() {
            info!("Stopping shard {}", shard_id);
            ShardWriterService::stop(shard).await;
        }
    }

    pub async fn load_shards(&self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Recovering shards from {}...", Configuration::shards_path());
        for entry in std::fs::read_dir(Configuration::shards_path())? {
            let entry = entry?;
            let shard_id = String::from(entry.file_name().to_str().unwrap());

            let shard: ShardWriterService = ShardWriterService::start(&shard_id.to_string())
                .await
                .map_err(|e| e.as_tonic_status())?;
            self.shards.write().await.insert(shard_id.clone(), shard);
            info!("Shard loaded: {:?}", shard_id);
        }
        Ok(())
    }

    async fn load_shard(&self, shard_id: &str) {
        info!("{}: Loading shard", shard_id);
        let in_memory = self.shards.read().await.contains_key(shard_id);
        if !in_memory {
            info!("{}: Shard was in memory", shard_id);
            let in_disk = Path::new(&Configuration::shards_path_id(shard_id)).exists();
            if in_disk {
                info!("{}: Shard was in disk", shard_id);
                let shard = ShardWriterService::start(shard_id).await.unwrap();
                info!("{}: Loaded shard", shard_id);
                self.shards
                    .write()
                    .await
                    .insert(shard_id.to_string(), shard);
                info!("{}: Inserted on memory", shard_id);
            }
        }
    }
}

impl Default for NodeWriterService {
    fn default() -> Self {
        Self::new()
    }
}

#[tonic::async_trait]
impl NodeWriter for NodeWriterService {
    async fn get_shard(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<ShardId>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        info!("{:?}: gRPC get_shard", request);
        let shard_request = request.into_inner();
        let shard_id = shard_request.id.as_str();
        self.load_shard(shard_id).await;
        info!("{:?}: Ready reading", shard_id);

        match self.shards.read().await.get(shard_id) {
            Some(shard) => {
                info!("{:?}: Ready readed", shard_id);

                let result_shard = ShardId {
                    id: String::from(&shard.id),
                };
                Ok(tonic::Response::new(result_shard))
            }
            None => {
                let message = format!("Shard not found {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn new_shard(
        &self,
        request: tonic::Request<EmptyQuery>,
    ) -> Result<tonic::Response<ShardCreated>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        info!("Creating new shard");
        let new_id = Uuid::new_v4().to_string();
        let new_shard = ShardWriterService::start(&new_id).await.unwrap();

        let document_version = new_shard.document_service_version;
        let vector_version = new_shard.vector_service_version;
        let paragraph_version = new_shard.paragraph_service_version;
        let relation_version = new_shard.relation_service_version;
        match self.shards.write().await.insert(new_id.clone(), new_shard) {
            Some(_) => Err(tonic::Status::internal(
                "A shard with this uuid already exists.".to_string(),
            )),
            None => Ok(tonic::Response::new(ShardCreated {
                id: new_id,
                document_service: document_version,
                paragraph_service: paragraph_version,
                vector_service: vector_version,
                relation_service: relation_version,
            })),
        }
    }

    async fn delete_shard(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<ShardId>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        info!("gRPC delete_shard {:?}", request);

        let delete_request = request.into_inner();
        let shard_id = &delete_request.id;
        self.load_shard(shard_id).await;

        let result = match self.shards.write().await.get_mut(shard_id) {
            Some(shard) => match shard.delete().await {
                Err(e) => {
                    let error_msg = format!("Error deleting shard {}: {}", shard_id, e);
                    error!("{}", error_msg);
                    Err(tonic::Status::internal(error_msg))
                }
                Ok(_) => Ok(tonic::Response::new(ShardId {
                    id: String::from(shard_id),
                })),
            },
            None => {
                let message = format!("Shard not found {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        };

        // We need to delete the shard
        match self.shards.write().await.remove(shard_id) {
            Some(shard) => info!("Remove {} from the DashMap succesfully.", shard.id),
            None => error!("Error removing shard from DashMap."),
        }

        result
    }

    async fn list_shards(
        &self,
        request: tonic::Request<EmptyQuery>,
    ) -> Result<tonic::Response<ShardIds>, tonic::Status> {
        info!("Listing shards");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let shards = self.shards.read().await;
        let mut result: Vec<ShardId> = Vec::with_capacity(shards.len());

        for (shard_id, _service) in shards.iter() {
            let result_shard = ShardId {
                id: shard_id.to_string(),
            };
            result.push(result_shard);
        }
        info!("Shards listed");
        Ok(tonic::Response::new(ShardIds { ids: result }))
    }

    // Incremental call that can be call multiple times for the same resource
    async fn set_resource(
        &self,
        request: tonic::Request<Resource>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        info!("Set resource starts");
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let resource = request.into_inner();
        let shard_id = &resource.shard_id;
        self.load_shard(shard_id).await;

        match self.shards.write().await.get_mut(shard_id) {
            Some(shard) => match shard.set_resource(&resource).await {
                Ok(_) => {
                    info!("Set resource ends correctly");
                    let count = shard.count();
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: count as u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(status))
                }
                Err(e) => {
                    let status = op_status::Status::Error as i32;
                    let detail = format!("Error: {}", e);
                    let op_status = OpStatus {
                        status,
                        detail,
                        count: 0_u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(op_status))
                }
            },
            None => {
                let message = format!("Error loading shard {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn remove_resource(
        &self,
        request: tonic::Request<ResourceId>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        let resource = request.into_inner();
        let shard_id = &resource.shard_id;
        self.load_shard(shard_id).await;
        match self.shards.write().await.get_mut(shard_id) {
            Some(shard) => match shard.remove_resource(&resource).await {
                Ok(_) => {
                    info!("remove resource ends correctly");
                    let count = shard.count();
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: count as u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(status))
                }
                Err(e) => {
                    let status = op_status::Status::Error as i32;
                    let detail = format!("Error: {}", e);
                    let op_status = OpStatus {
                        status,
                        detail,
                        count: 0_u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(op_status))
                }
            },
            None => {
                let message = format!("Shard not found {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn set_relations(
        &self,
        request: tonic::Request<SetRelationsRequest>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        todo!()
    }

    async fn del_relations(
        &self,
        request: tonic::Request<DelRelationsRequest>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);
        todo!()
    }

    async fn set_vectors_field(
        &self,
        request: tonic::Request<SetVectorFieldRequest>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let vector_request = request.into_inner();
        let shard_id = &vector_request.shard_id;
        self.load_shard(shard_id).await;

        match self.shards.write().await.get_mut(shard_id) {
            Some(shard) => match shard.set_vector_field(&vector_request).await {
                Ok(_) => {
                    let status = OpStatus {
                        status: 0,
                        detail: "Success!".to_string(),
                        count: 0_u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(status))
                }
                Err(e) => {
                    let status = op_status::Status::Error as i32;
                    let detail = format!("Error: {}", e);
                    let op_status = OpStatus {
                        status,
                        detail,
                        count: 0_u64,
                        shard_id: shard_id.to_string(),
                    };
                    Ok(tonic::Response::new(op_status))
                }
            },
            None => {
                let message = format!("Shard not found {}", shard_id);
                Err(tonic::Status::not_found(message))
            }
        }
    }

    async fn del_vectors_field(
        &self,
        request: tonic::Request<DelVectorFieldRequest>,
    ) -> Result<tonic::Response<OpStatus>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let _vector_request = request.into_inner();

        todo!()
    }

    async fn gc(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EmptyResponse>, tonic::Status> {
        let parent_cx =
            global::get_text_map_propagator(|prop| prop.extract(&MetadataMap(request.metadata())));
        Span::current().set_parent(parent_cx);

        let _vector_request = request.into_inner();
        todo!()
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
        let node_writer = NodeWriterService::new();
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

        for id in request_ids {
            let response = client
                .delete_shard(Request::new(ShardId { id: id.clone() }))
                .await
                .expect("Error in delete_shard request");
            let deleted_id = response.get_ref().id.clone();
            assert_eq!(deleted_id, id);
        }

        let response = client
            .list_shards(Request::new(EmptyQuery {}))
            .await
            .expect("Error in list_shards request");

        assert_eq!(response.get_ref().ids.len(), 0);

        Ok(())
    }
}
