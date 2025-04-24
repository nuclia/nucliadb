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
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

use crate::errors::NidxError;
use crate::metadata::{Index, IndexId, IndexKind, Shard};
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use nidx_protos::nidx::nidx_api_server::*;
use nidx_protos::*;
use nidx_vector::config::VectorConfig;
use object_store::DynObjectStore;
use tokio::sync::mpsc::Sender;
use tokio_stream::wrappers::ReceiverStream;
use tonic::service::Routes;
use tonic::{Request, Response, Result, Status};
use tracing::error;
use uuid::Uuid;

use crate::api::shards;
use crate::{NidxMetadata, Settings, import_export};

#[derive(Clone)]
pub struct ApiServer {
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
}

impl ApiServer {
    pub fn new(settings: &Settings) -> Self {
        Self {
            meta: settings.metadata.clone(),
            storage: settings.storage.as_ref().unwrap().object_store.clone(),
        }
    }

    pub fn into_router(self) -> axum::Router {
        let myself = self.clone();
        let myself2 = self.clone();
        Routes::new(NidxApiServer::new(self))
            .into_axum_router()
            .route(
                "/api/shard/:shard_id/download",
                axum::routing::get(download_shard).with_state(myself),
            )
            .route(
                "/api/index/:index_id/download",
                axum::routing::get(download_index).with_state(myself2),
            )
    }
}

#[tonic::async_trait]
impl NidxApi for ApiServer {
    async fn get_shard(&self, request: Request<GetShardRequest>) -> Result<Response<noderesources::Shard>> {
        let request = request.into_inner();
        let shard_id = request
            .shard_id
            .ok_or(Status::invalid_argument("Shard ID required"))?
            .id;
        let shard_id = Uuid::parse_str(&shard_id).map_err(NidxError::from)?;

        let shard = Shard::get(&self.meta.pool, shard_id).await.map_err(NidxError::from)?;
        let index_stats = shard.stats(&self.meta.pool).await.map_err(NidxError::from)?;

        Ok(Response::new(noderesources::Shard {
            metadata: Some(ShardMetadata {
                kbid: shard.kbid.to_string(),
                release_channel: 0,
            }),
            shard_id: shard_id.to_string(),
            fields: index_stats.get(&IndexKind::Text).map(|s| s.records).unwrap_or(0) as u64,
            paragraphs: index_stats.get(&IndexKind::Paragraph).map(|s| s.records).unwrap_or(0) as u64,
            sentences: index_stats.get(&IndexKind::Vector).map(|s| s.records).unwrap_or(0) as u64,
            size_bytes: index_stats.values().map(|i| i.size_bytes).sum::<i64>() as u64,
        }))
    }

    async fn new_shard(&self, request: Request<NewShardRequest>) -> Result<Response<ShardCreated>> {
        // TODO? analytics event
        let request = request.into_inner();
        let kbid = Uuid::from_str(&request.kbid).map_err(NidxError::from)?;
        let mut vector_configs = HashMap::with_capacity(request.vectorsets_configs.len());
        for (vectorset_id, config) in request.vectorsets_configs {
            vector_configs.insert(
                vectorset_id,
                VectorConfig::try_from(config).map_err(|e| Status::internal(e.to_string()))?,
            );
        }

        let shard = shards::create_shard(&self.meta, kbid, vector_configs).await?;

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
            ids: ids.iter().map(|x| ShardId { id: x.to_string() }).collect(),
        }))
    }

    async fn add_vector_set(&self, request: Request<NewVectorSetRequest>) -> Result<Response<OpStatus>> {
        let request = request.into_inner();
        let Some(VectorSetId {
            shard: Some(ShardId { id: ref shard_id }),
            ref vectorset,
        }) = request.id
        else {
            return Err(NidxError::invalid("Vectorset ID is required").into());
        };
        let shard_id = Uuid::from_str(shard_id).map_err(NidxError::from)?;
        let config = if let Some(config) = request.config {
            VectorConfig::try_from(config)
                .map_err(|error| NidxError::invalid(&format!("Invalid vectorset configuration: {error:?}")))?
        } else {
            return Err(NidxError::invalid("Vectorset configuration is required").into());
        };

        Index::create(&self.meta.pool, shard_id, vectorset, config.into())
            .await
            .map_err(NidxError::from)?;

        Ok(Response::new(OpStatus {
            status: op_status::Status::Ok.into(),
            detail: "Vectorset successfully created".to_string(),
            ..Default::default()
        }))
    }

    async fn remove_vector_set(&self, request: Request<VectorSetId>) -> Result<Response<OpStatus>> {
        let VectorSetId {
            shard: Some(ShardId { id: ref shard_id }),
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
        let indexes = Index::for_shard(&self.meta.pool, shard_id)
            .await
            .map_err(NidxError::from)?;

        let vectorsets = indexes
            .into_iter()
            .filter(|index| index.kind == IndexKind::Vector)
            .map(|index| index.name)
            .collect();
        Ok(tonic::Response::new(VectorSetList {
            shard: Some(request),
            vectorsets,
        }))
    }
}

struct ChannelWriter(Sender<anyhow::Result<Bytes>>);

impl Write for ChannelWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if let Err(e) = self.0.blocking_send(Ok(Bytes::copy_from_slice(buf))) {
            return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe, e));
        };

        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn download_shard(State(server): State<ApiServer>, Path(shard_id): Path<Uuid>) -> impl IntoResponse {
    let shard = Shard::get(&server.meta.pool, shard_id).await;
    if let Err(e) = shard {
        return axum::response::Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from(e.to_string()))
            .unwrap();
    };

    download_export(server, shard_id, None, shard_id.to_string()).await
}

async fn download_index(State(server): State<ApiServer>, Path(index_id): Path<i64>) -> impl IntoResponse {
    let index = match Index::get(&server.meta.pool, index_id.into()).await {
        Ok(index) => index,
        Err(e) => {
            return axum::response::Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Body::from(e.to_string()))
                .unwrap();
        }
    };

    download_export(server, index.shard_id, Some(index.id), index_id.to_string()).await
}

async fn download_export(
    server: ApiServer,
    shard_id: Uuid,
    index_id: Option<IndexId>,
    filename: String,
) -> axum::response::Response<axum::body::Body> {
    let (tx, rx) = tokio::sync::mpsc::channel(10);
    let txc = tx.clone();
    let writer = ChannelWriter(tx);

    tokio::spawn(async move {
        let r = import_export::export_shard(server.meta, server.storage, shard_id, index_id, writer).await;
        // If an error happens during the export, send it to the stream so axum cancels the download
        // and the user gets an error. Status code will still be 200 since this happens in the middle
        // of a stream, but it's better than nothing
        if let Err(e) = r {
            error!("Error exporting shard: {e:?}");
            let _ = txc.send(Err(e)).await;
        }
    });

    let body = Body::from_stream(ReceiverStream::new(rx));
    axum::response::Response::builder()
        .header(
            "Content-Disposition",
            format!("attachment; filename=\"{filename}.tar.zstd\""),
        )
        .body(body)
        .unwrap()
}
