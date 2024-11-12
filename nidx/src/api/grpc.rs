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
use std::str::FromStr;

use crate::errors::NidxError;
use crate::grpc_server::RemappedGrpcService;
use crate::metadata::{Index, IndexKind, Shard
};
use nidx_protos::nidx::nidx_api_server::*;
use nidx_protos::*;
use nidx_vector::config::VectorConfig;
use tonic::service::Routes;
use tonic::{Request, Response, Result, Status};
use uuid::Uuid;

use crate::api::shards;
use crate::NidxMetadata;

pub struct ApiServer {
    meta: NidxMetadata,
}

impl ApiServer {
    pub fn new(meta: NidxMetadata) -> Self {
        Self {
            meta,
        }
    }

    pub fn into_service(self) -> RemappedGrpcService {
        RemappedGrpcService {
            routes: Routes::new(NidxApiServer::new(self)),
            package: "nidx.NidxApi".to_string(),
        }
    }
}

#[tonic::async_trait]
impl NidxApi for ApiServer {
    async fn get_shard(&self, request: Request<GetShardRequest>) -> Result<Response<noderesources::Shard>> {
        let request = request.into_inner();
        let shard_id = request.shard_id.ok_or(Status::invalid_argument("Shard ID required"))?.id;
        let shard_id = Uuid::parse_str(&shard_id).map_err(NidxError::from)?;

        let shard = Shard::get(&self.meta.pool, shard_id).await.map_err(NidxError::from)?;
        let index_stats = shard.stats(&self.meta.pool).await.map_err(NidxError::from)?;

        Ok(Response::new(noderesources::Shard {
            metadata: Some(ShardMetadata {
                kbid: shard.kbid.to_string(),
                release_channel: 0,
            }),
            shard_id: shard_id.to_string(),
            fields: *index_stats.get(&IndexKind::Text).unwrap_or(&0) as u64,
            paragraphs: *index_stats.get(&IndexKind::Paragraph).unwrap_or(&0) as u64,
            sentences: *index_stats.get(&IndexKind::Vector).unwrap_or(&0) as u64,
        }))
    }

    async fn new_shard(&self, request: Request<NewShardRequest>) -> Result<Response<ShardCreated>> {
        // TODO? analytics event
        let request = request.into_inner();
        let kbid = Uuid::from_str(&request.kbid).map_err(NidxError::from)?;
        let mut vector_configs = HashMap::with_capacity(request.vectorsets_configs.len());
        for (vectorset_id, config) in request.vectorsets_configs {
            vector_configs
                .insert(vectorset_id, VectorConfig::try_from(config).map_err(|e| Status::internal(e.to_string()))?);
        }

        let shard = shards::create_shard(&self.meta, kbid, vector_configs).await.map_err(NidxError::from)?;

        Ok(Response::new(ShardCreated {
            id: shard.id.to_string(),
            // TODO: index versions
            ..Default::default()
        }))
    }

    async fn delete_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>> {
        // TODO? analytics event
        let request = request.into_inner();
        let shard_id = Uuid::from_str(&request.id).map_err(NidxError::from)?;

        shards::delete_shard(&self.meta, shard_id).await?;

        Ok(Response::new(ShardId {
            id: shard_id.to_string(),
        }))
    }

    async fn list_shards(&self, _request: Request<EmptyQuery>) -> Result<Response<ShardIds>> {
        let ids = Shard::list_ids(&self.meta.pool).await.map_err(NidxError::from)?;
        Ok(Response::new(ShardIds {
            ids: ids
                .iter()
                .map(|x| ShardId {
                    id: x.to_string(),
                })
                .collect(),
        }))
    }

    async fn add_vector_set(&self, request: Request<NewVectorSetRequest>) -> Result<Response<OpStatus>> {
        let request = request.into_inner();
        let Some(VectorSetId {
            shard: Some(ShardId {
                id: ref shard_id,
            }),
            ref vectorset,
        }) = request.id
        else {
            return Err(NidxError::invalid("Vectorset ID is required").into());
        };
        let shard_id = Uuid::from_str(shard_id).map_err(NidxError::from)?;
        let config = if let Some(config) = request.config {
            VectorConfig::try_from(config).map_err(|error| NidxError::invalid(&format!("Invalid vectorset configuration: {error:?}")))?
        } else {
            return Err(NidxError::invalid("Vectorset configuration is required").into());
        };

        Index::create(&self.meta.pool, shard_id, vectorset, config.into()).await.map_err(NidxError::from)?;

        Ok(Response::new(OpStatus {
            status: op_status::Status::Ok.into(),
            detail: "Vectorset successfully created".to_string(),
            ..Default::default()
        }))
    }

    async fn remove_vector_set(&self, request: Request<VectorSetId>) -> Result<Response<OpStatus>> {
        let VectorSetId {
            shard: Some(ShardId {
                id: ref shard_id,
            }),
            ref vectorset,
        } = request.into_inner()
        else {
            return Err(NidxError::invalid("Vectorset ID is required").into());
        };
        let shard_id = Uuid::from_str(shard_id).map_err(NidxError::from)?;

        shards::delete_vectorset(&self.meta, shard_id, vectorset).await?;

        Ok(Response::new(OpStatus {
            status: op_status::Status::Ok.into(),
            detail: "Vectorset successfully deleted".to_string(),
            ..Default::default()
        }))
    }

    async fn list_vector_sets(&self, request: Request<ShardId>) -> Result<Response<VectorSetList>> {
        let request = request.into_inner();
        let shard_id = Uuid::from_str(&request.id).map_err(NidxError::from)?;
        // TODO: query only vector indexes
        let indexes = Index::for_shard(&self.meta.pool, shard_id).await.map_err(NidxError::from)?;

        let vectorsets = indexes.into_iter().filter(|index| index.kind == IndexKind::Vector).map(|index| index.name).collect();
        Ok(tonic::Response::new(VectorSetList {
            shard: Some(request),
            vectorsets,
        }))
    }

    async fn get_metadata(&self, _request: Request<EmptyQuery>) -> Result<Response<NodeMetadata>> {
        // TODO
        Ok(Response::new(NodeMetadata::default()))
    }
}
