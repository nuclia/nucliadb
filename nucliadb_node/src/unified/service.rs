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

use std::cmp::min;
use std::io::Read;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use super::node::ShardWriter;
use crate::unified::replication;
use nucliadb_index::index;
use nucliadb_protos::unified;
use std::fs::OpenOptions;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use super::shards;

// max number of commits to send at a time to the secondary node
const REPLICATION_COMMIT_SIZE: u16 = 200;

pub struct NodeGRPCDriver {
    shard_manager: Arc<Mutex<shards::ShardManager>>,
    primary_replicator: Option<Arc<Mutex<replication::PrimaryReplicator>>>,
}

impl NodeGRPCDriver {
    // pub fn new(settings: Arc<Settings>) -> Self {
    //     let mut primary_replicator = None;
    //     if settings.node_role() == NodeRole::Primary {
    //         primary_replicator = Some(Arc::new(Mutex::new(replication::PrimaryReplicator::new(
    //             settings.clone(),
    //         ))));
    //     }
    //     return Self {
    //         shard_manager: Arc::new(Mutex::new(shards::ShardManager::new())),
    //         primary_replicator: primary_replicator,
    //     };
    // }
    pub fn new(
        shard_manager: Arc<Mutex<shards::ShardManager>>,
        primary_replicator: Option<Arc<Mutex<replication::PrimaryReplicator>>>,
    ) -> Self {
        return Self {
            shard_manager,
            primary_replicator,
        };
    }
}

#[tonic::async_trait]
impl unified::node_service_server::NodeService for NodeGRPCDriver {
    type ReplicateStream = ReceiverStream<Result<unified::PrimaryReplicateResponse, tonic::Status>>;
    type DownloadSegmentStream =
        ReceiverStream<Result<unified::DownloadSegmentResponse, tonic::Status>>;

    async fn replicate(
        &self,
        request: tonic::Request<tonic::Streaming<unified::SecondaryReplicateRequest>>,
    ) -> Result<tonic::Response<Self::ReplicateStream>, tonic::Status> {
        if self.primary_replicator.is_none() {
            return Err(tonic::Status::unimplemented("Not a primary node"));
        }
        let replicator = self.primary_replicator.as_ref().unwrap().clone();

        let mut stream = request.into_inner();
        let receiver = tokio::sync::mpsc::channel(4);
        let sender = receiver.0.clone();
        tokio::spawn(async move {
            while let Some(req) = stream.message().await.unwrap() {
                for rep_pos in req.positions {
                    replicator.lock().unwrap().secondary_committed(
                        req.secondary_id.as_str(),
                        rep_pos.shard_id.as_str(),
                        rep_pos.position,
                    );
                }
                let commits = replicator
                    .lock()
                    .unwrap()
                    .get_commits(req.secondary_id.as_str(), REPLICATION_COMMIT_SIZE);
                sender
                    .send(Ok(unified::PrimaryReplicateResponse {
                        commits: commits
                            .into_iter()
                            .map(|c| unified::PrimaryReplicateCommit {
                                shard_id: c.shard_id,
                                position: c.position,
                                segments: c.segments,
                            })
                            .collect(),
                    }))
                    .await
                    .unwrap();
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver.1)))
    }

    async fn download_segment(
        &self,
        request: tonic::Request<unified::DownloadSegmentRequest>,
    ) -> Result<tonic::Response<Self::DownloadSegmentStream>, tonic::Status> {
        if self.primary_replicator.is_none() {
            return Err(tonic::Status::unimplemented("Not a primary node"));
        }
        let rdata = request.into_inner();
        let filepath = self
            .primary_replicator
            .as_ref()
            .unwrap()
            .clone()
            .lock()
            .unwrap()
            .get_segment_filepath(rdata.shard_id.as_str(), rdata.segment_id.as_str());
        let chunk_size = rdata.chunk_size.clone();

        let receiver = tokio::sync::mpsc::channel(4);
        let sender = receiver.0.clone();
        tokio::spawn(async move {
            let mut total = 0;
            let mut chunk = 0;
            let mut file = OpenOptions::new().read(true).open(filepath).unwrap();
            let filesize = file.metadata().unwrap().len();

            loop {
                let vec_size = min(chunk_size, filesize - total);
                total += chunk_size;
                let mut buf = vec![0; vec_size as usize];
                file.read_exact(buf.as_mut_slice()).unwrap();
                // file.read_exact(&mut buf).unwrap();
                let reply = unified::DownloadSegmentResponse {
                    data: buf,
                    chunk: chunk,
                    read_position: total,
                    total_size: filesize,
                };
                chunk += 1;
                sender.send(Ok(reply)).await.unwrap();
                if total >= filesize {
                    break;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(receiver.1)))
    }

    async fn create_shard(
        &self,
        request: Request<unified::CreateShardRequest>,
    ) -> Result<Response<unified::CreateShardResponse>, Status> {
        let _ = self
            .shard_manager
            .lock()
            .unwrap()
            .create_shard(request.into_inner().shard_id.as_str());
        Ok(Response::new(unified::CreateShardResponse {}))
    }

    async fn delete_shard(
        &self,
        request: tonic::Request<unified::DeleteShardRequest>,
    ) -> Result<tonic::Response<unified::DeleteShardResponse>, tonic::Status> {
        let _ = self
            .shard_manager
            .lock()
            .unwrap()
            .delete_shard(request.into_inner().shard_id.as_str());
        Ok(Response::new(unified::DeleteShardResponse {}))
    }

    async fn index(
        &self,
        request: tonic::Request<unified::IndexRequest>,
    ) -> Result<tonic::Response<unified::IndexResponse>, tonic::Status> {
        if self.primary_replicator.is_none() {
            return Err(tonic::Status::unimplemented("Not a primary node"));
        }

        let mut writer = ShardWriter::new(
            self.shard_manager.clone(),
            self.primary_replicator.as_ref().unwrap().clone(),
        );

        let body = request.into_inner();
        writer
            .index_resource(
                body.shard_id.as_str(),
                index::ResourceData {
                    // will need data mapping from grpc -> index
                    id: body.resource_id,
                },
            )
            .unwrap();

        Ok(Response::new(unified::IndexResponse {}))
    }

    async fn delete(
        &self,
        request: tonic::Request<unified::DeleteRequest>,
    ) -> Result<tonic::Response<unified::DeleteResponse>, tonic::Status> {
        if self.primary_replicator.is_none() {
            return Err(tonic::Status::unimplemented("Not a primary node"));
        }

        let mut writer = ShardWriter::new(
            self.shard_manager.clone(),
            self.primary_replicator.as_ref().unwrap().clone(),
        );

        let body = request.into_inner();
        writer
            .delete_resource(body.shard_id.as_str(), body.resource_id.as_str())
            .unwrap();
        Ok(Response::new(unified::DeleteResponse {}))
    }

    async fn search(
        &self,
        request: tonic::Request<unified::SearchRequest>,
    ) -> Result<tonic::Response<unified::SearchResponse>, tonic::Status> {
        let body = request.into_inner();
        let index_shard = self
            .shard_manager
            .lock()
            .unwrap()
            .get_shard(body.shard_id.as_str())
            .unwrap();

        let search_resp = index_shard
            .lock()
            .unwrap()
            .search(index::SearchRequest {
                query: body.query,
                vector: body.vector,
                limit: body.limit,
                offset: body.offset,
            })
            .unwrap();

        Ok(Response::new(unified::SearchResponse {
            items: search_resp
                .items
                .into_iter()
                .map(|r| unified::ResultItem {
                    resource_id: r.resource_id,
                    field_id: r.field_id,
                    score: r.score,
                    score_type: r.score_type,
                })
                .collect(),
            total: search_resp.total,
        }))
    }
}

pub async fn run_server(
    address: SocketAddr,
    shard_manager: Arc<Mutex<shards::ShardManager>>,
    primary_replicator: Option<Arc<Mutex<replication::PrimaryReplicator>>>,
) {
    let service = NodeGRPCDriver::new(shard_manager, primary_replicator);
    let server = unified::node_service_server::NodeServiceServer::new(service);

    println!("server listening on {}", address);

    let _ = Server::builder()
        // GrpcWeb is over http1 so we must enable it.
        .accept_http1(true)
        .add_service(server)
        .serve(address)
        .await;
}
