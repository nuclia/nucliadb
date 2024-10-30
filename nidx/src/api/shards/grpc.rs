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

use super::ShardsServer;

use std::collections::HashMap;
use std::str::FromStr;

use nidx_protos::node_writer_server::{NodeWriter, NodeWriterServer};
use nidx_protos::*;
use nidx_vector::config::VectorConfig;
use tonic::{transport::Server, Request, Response, Status};
use uuid::Uuid;

impl ShardsServer {
    pub async fn serve(self) {
        Server::builder()
            .add_service(NodeWriterServer::new(self))
            .serve("0.0.0.0:10000".parse().unwrap())
            .await
            .unwrap();
    }
}

#[tonic::async_trait]
impl NodeWriter for ShardsServer {
    async fn new_shard(&self, request: Request<NewShardRequest>) -> Result<Response<ShardCreated>, Status> {
        // TODO? analytics event
        let request = request.into_inner();
        let kbid = Uuid::from_str(&request.kbid).map_err(|e| Status::internal(e.to_string()))?;
        let mut vector_configs = HashMap::with_capacity(request.vectorsets_configs.len());
        for (vectorset_id, config) in request.vectorsets_configs {
            vector_configs
                .insert(vectorset_id, VectorConfig::try_from(config).map_err(|e| Status::internal(e.to_string()))?);
        }

        let shard = self.create_shard(kbid, vector_configs).await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ShardCreated {
            id: shard.id.to_string(),
            // TODO: index versions
            ..Default::default()
        }))
    }

    async fn delete_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>, Status> {
        // TODO? analytics event
        let request = request.into_inner();
        let shard_id = Uuid::from_str(&request.id).map_err(|e| Status::internal(e.to_string()))?;

        self.delete_shard(shard_id).await.map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(ShardId {
            id: shard_id.to_string(),
        }))
    }

    async fn list_shards(&self, _request: Request<EmptyQuery>) -> Result<Response<ShardIds>, Status> {
        todo!()
    }

    async fn set_resource(&self, _request: Request<Resource>) -> Result<Response<OpStatus>, Status> {
        todo!()
    }

    async fn set_resource_from_storage(&self, _request: Request<IndexMessage>) -> Result<Response<OpStatus>, Status> {
        todo!()
    }

    async fn remove_resource(&self, _request: Request<ResourceId>) -> Result<Response<OpStatus>, Status> {
        todo!()
    }

    async fn add_vector_set(&self, _request: Request<NewVectorSetRequest>) -> Result<Response<OpStatus>, Status> {
        todo!()
    }

    async fn remove_vector_set(&self, _request: Request<VectorSetId>) -> Result<Response<OpStatus>, Status> {
        todo!()
    }

    async fn list_vector_sets(&self, _request: Request<ShardId>) -> Result<Response<VectorSetList>, Status> {
        todo!()
    }

    async fn get_metadata(&self, _request: Request<EmptyQuery>) -> Result<Response<NodeMetadata>, Status> {
        todo!()
    }

    async fn gc(&self, _request: Request<ShardId>) -> Result<Response<GarbageCollectorResponse>, Status> {
        todo!()
    }

    async fn merge(&self, _request: Request<ShardId>) -> Result<Response<MergeResponse>, Status> {
        todo!()
    }
}
