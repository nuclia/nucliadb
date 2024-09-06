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

use crate::cache::ShardReaderCache;
use crate::errors::ShardNotFoundError;
use crate::grpc::update::{update_loop, UpdateParameters};
use crate::settings::Settings;
use crate::shards::reader::{ShardFileChunkIterator, ShardReader};
use crate::telemetry::run_with_telemetry;
use nucliadb_core::paragraphs::ParagraphIterator;
use nucliadb_core::protos::node_reader_server::NodeReader;
use nucliadb_core::protos::*;
use nucliadb_core::texts::DocumentIterator;
use nucliadb_core::tracing::{info_span, Span};
use std::sync::Arc;
use std::time::Duration;
use tokio::task::JoinHandle;
use Shard as ShardPB;

const UPDATE_REFRESH_RATE: &str = "UPDATE_REFRESH_RATE_MILLIS";

pub struct NodeReaderGRPCDriver {
    #[allow(unused)]
    update_loop_handle: JoinHandle<()>,
    shards: Arc<ShardReaderCache>,
}

impl NodeReaderGRPCDriver {
    pub fn new(settings: Settings) -> Self {
        let cache_settings = settings.clone();
        let shards = Arc::new(ShardReaderCache::new(cache_settings));
        let shards_update_loop_copy = Arc::clone(&shards);
        let update_refresh_rate = match std::env::var(UPDATE_REFRESH_RATE) {
            Ok(v) => Duration::from_millis(v.parse().unwrap_or(100)),
            Err(_) => Duration::from_millis(100),
        };
        let update_parameters = UpdateParameters {
            shards_path: settings.shards_path(),
            refresh_rate: update_refresh_rate,
        };
        let update_loop_handle = tokio::spawn(async move {
            update_loop(update_parameters, shards_update_loop_copy).await;
        });

        NodeReaderGRPCDriver {
            shards,
            update_loop_handle,
        }
    }

    async fn obtain_shard(&self, id: impl Into<String>) -> Result<Arc<ShardReader>, tonic::Status> {
        let id = id.into();
        let id_clone = id.clone();
        let shards = Arc::clone(&self.shards);

        tokio::task::spawn_blocking(move || obtain_shard(shards, id_clone))
            .await
            .map_err(|error| tonic::Status::internal(format!("Error loading shard {id}: {error:?}")))?
    }
}

fn obtain_shard(shards: Arc<ShardReaderCache>, id: impl Into<String>) -> Result<Arc<ShardReader>, tonic::Status> {
    let id = id.into();
    let shard = shards.get(&id).map_err(|error| {
        if error.is::<ShardNotFoundError>() {
            tonic::Status::not_found(error.to_string())
        } else {
            tonic::Status::internal(format!("Error loading shard {id}: {error:?}"))
        }
    })?;
    Ok(shard)
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

impl futures_core::Stream for GrpcStreaming<ShardFileChunkIterator> {
    type Item = Result<ShardFileChunk, tonic::Status>;

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
    type DownloadShardFileStream = GrpcStreaming<ShardFileChunkIterator>;

    async fn paragraphs(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::ParagraphsStream>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let shard = self.obtain_shard(shard_id).await?;
        match shard.paragraph_iterator(request) {
            Ok(iterator) => Ok(tonic::Response::new(GrpcStreaming(iterator))),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn documents(
        &self,
        request: tonic::Request<StreamRequest>,
    ) -> Result<tonic::Response<Self::DocumentsStream>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let shard = self.obtain_shard(shard_id).await?;
        match shard.document_iterator(request) {
            Ok(iterator) => Ok(tonic::Response::new(GrpcStreaming(iterator))),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn get_shard(
        &self,
        request: tonic::Request<GetShardRequest>,
    ) -> Result<tonic::Response<ShardPB>, tonic::Status> {
        let span = Span::current();
        let request = request.into_inner();
        let shard_id = match request.shard_id {
            Some(ref shard_id) => shard_id.id.clone(),
            None => return Err(tonic::Status::invalid_argument("Shard ID must be provided")),
        };
        let info = info_span!(parent: &span, "get shard");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_info())
        };
        let shard_info = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match shard_info {
            Ok(shard) => Ok(tonic::Response::new(shard)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn search(
        &self,
        request: tonic::Request<SearchRequest>,
    ) -> Result<tonic::Response<SearchResponse>, tonic::Status> {
        let span = Span::current();
        let search_request = request.into_inner();
        let shard_id = search_request.shard.clone();
        let info = info_span!(parent: &span, "search");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.search(search_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn suggest(
        &self,
        request: tonic::Request<SuggestRequest>,
    ) -> Result<tonic::Response<SuggestResponse>, tonic::Status> {
        let span = Span::current();
        let suggest_request = request.into_inner();
        let shard_id = suggest_request.shard.clone();
        let info = info_span!(parent: &span, "suggest");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.suggest(suggest_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn vector_search(
        &self,
        request: tonic::Request<VectorSearchRequest>,
    ) -> Result<tonic::Response<VectorSearchResponse>, tonic::Status> {
        let span = Span::current();
        let vector_request = request.into_inner();
        let shard_id = vector_request.id.clone();
        let info = info_span!(parent: &span, "vector search");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.vector_search(vector_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_search(
        &self,
        request: tonic::Request<RelationSearchRequest>,
    ) -> Result<tonic::Response<RelationSearchResponse>, tonic::Status> {
        let span = Span::current();
        let relation_request = request.into_inner();
        let shard_id = relation_request.shard_id.clone();
        let info = info_span!(parent: &span, "relations search");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.relation_search(relation_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn document_search(
        &self,
        request: tonic::Request<DocumentSearchRequest>,
    ) -> Result<tonic::Response<DocumentSearchResponse>, tonic::Status> {
        let span = Span::current();
        let document_request = request.into_inner();
        let shard_id = document_request.id.clone();
        let info = info_span!(parent: &span, "document search");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.document_search(document_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn paragraph_search(
        &self,
        request: tonic::Request<ParagraphSearchRequest>,
    ) -> Result<tonic::Response<ParagraphSearchResponse>, tonic::Status> {
        let span = Span::current();
        let paragraph_request = request.into_inner();
        let shard_id = paragraph_request.id.clone();
        let info = info_span!(parent: &span, "paragraph search");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.paragraph_search(paragraph_request))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn document_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let span = Span::current();
        let shard_id = request.into_inner().id;
        let info = info_span!(parent: &span, "document ids");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_text_keys())
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(ids) => Ok(tonic::Response::new(IdCollection {
                ids,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn paragraph_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let span = Span::current();
        let shard_id = request.into_inner().id;
        let info = info_span!(parent: &span, "paragraph ids");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_paragraphs_keys())
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(ids) => Ok(tonic::Response::new(IdCollection {
                ids,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn vector_ids(
        &self,
        request: tonic::Request<VectorSetId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let span = Span::current();
        let info = info_span!(parent: &span, "vector ids");

        let request = request.into_inner();
        let VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset: vectorset_id,
        } = request
        else {
            return Err(tonic::Status::invalid_argument("Shard ID must be provided"));
        };
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_vectors_keys(&vectorset_id))
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;

        match response {
            Ok(ids) => Ok(tonic::Response::new(IdCollection {
                ids,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_ids(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<IdCollection>, tonic::Status> {
        let span = Span::current();
        let shard_id = request.into_inner().id;
        let info = info_span!(parent: &span, "relation ids");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_relations_keys())
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(ids) => Ok(tonic::Response::new(IdCollection {
                ids,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn relation_edges(
        &self,
        request: tonic::Request<ShardId>,
    ) -> Result<tonic::Response<EdgeList>, tonic::Status> {
        let span = Span::current();
        let shard_id = request.into_inner().id;
        let info = info_span!(parent: &span, "relation edges");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_relations_edges())
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(response) => Ok(tonic::Response::new(response)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn get_shard_files(
        &self,
        request: tonic::Request<GetShardFilesRequest>,
    ) -> Result<tonic::Response<ShardFileList>, tonic::Status> {
        let span = Span::current();
        let shard_id = request.into_inner().shard_id;
        let info = info_span!(parent: &span, "get shard files");
        let shards = Arc::clone(&self.shards);
        let task = move || {
            let shard = obtain_shard(shards, shard_id)?;
            run_with_telemetry(info, move || shard.get_shard_files())
        };
        let response = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match response {
            Ok(filelist) => Ok(tonic::Response::new(filelist)),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn download_shard_file(
        &self,
        request: tonic::Request<DownloadShardFileRequest>,
    ) -> Result<tonic::Response<Self::DownloadShardFileStream>, tonic::Status> {
        let request = request.into_inner();
        let shard_id = request.shard_id;
        let path = request.relative_path;
        let shard = self.obtain_shard(shard_id.clone()).await?;
        match shard.download_file_iterator(path) {
            Ok(iterator) => Ok(tonic::Response::new(GrpcStreaming(iterator))),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }
}
