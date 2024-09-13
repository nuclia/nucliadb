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

use crate::analytics::payload::AnalyticsEvent;
use crate::analytics::sync::send_analytics_event;
use crate::cache::ShardWriterCache;
use crate::errors::ShardNotFoundError;
use crate::grpc::collect_garbage::{garbage_collection_loop, GCParameters};
use crate::merge::{global_merger, MergePriority, MergeRequest, MergeWaiter};
use crate::settings::Settings;
use crate::shards::indexes::DEFAULT_VECTORS_INDEX_NAME;
use crate::shards::writer::{NewShard, ShardWriter};
use crate::telemetry::run_with_telemetry;
use crate::utils::{get_primary_node_id, list_shards, read_host_key};
use nucliadb_core::metrics::get_metrics;
use nucliadb_core::protos::node_writer_server::NodeWriter;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::{
    garbage_collector_response, merge_response, op_status, EmptyQuery, GarbageCollectorResponse, IndexMessage,
    MergeResponse, NewShardRequest, NewVectorSetRequest, NodeMetadata, OpStatus, Resource, ResourceId, ShardCreated,
    ShardId, ShardIds, VectorSetId, VectorSetList,
};
use nucliadb_core::tracing::{self, Span, *};
use nucliadb_core::NodeResult;
use nucliadb_vectors::config::VectorConfig;
use object_store::path::Path;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task::JoinHandle;
use tonic::{Request, Response, Status};

const GC_LOOP_INTERVAL: &str = "GC_INTERVAL_SECS";

pub struct NodeWriterGRPCDriver {
    #[allow(unused)]
    gc_loop_handle: JoinHandle<()>,
    shards: Arc<ShardWriterCache>,
    sender: Option<UnboundedSender<NodeWriterEvent>>,
    settings: Settings,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeWriterEvent {
    ShardCreation,
    ShardDeletion,
}

impl NodeWriterGRPCDriver {
    pub fn new(settings: Settings, shard_cache: Arc<ShardWriterCache>) -> Self {
        let cache_gc_copy = Arc::clone(&shard_cache);
        let gc_loop_interval = match std::env::var(GC_LOOP_INTERVAL) {
            Ok(v) => Duration::from_secs(v.parse().unwrap_or(1)),
            Err(_) => Duration::from_secs(1),
        };
        let gc_parameters = GCParameters {
            shards_path: settings.shards_path(),
            loop_interval: gc_loop_interval,
        };
        let gc_loop_handle = tokio::spawn(async move {
            garbage_collection_loop(gc_parameters, cache_gc_copy).await;
        });

        NodeWriterGRPCDriver {
            settings,
            gc_loop_handle,
            shards: shard_cache,
            sender: None,
        }
    }

    pub fn with_sender(self, sender: UnboundedSender<NodeWriterEvent>) -> Self {
        NodeWriterGRPCDriver {
            sender: Some(sender),
            ..self
        }
    }

    #[tracing::instrument(skip_all)]
    fn emit_event(&self, event: NodeWriterEvent) {
        if let Some(sender) = &self.sender {
            let _ = sender.send(event);
        }
    }
}

fn obtain_shard(shards: Arc<ShardWriterCache>, id: impl Into<String>) -> Result<Arc<ShardWriter>, tonic::Status> {
    let id = id.into();

    let shard = shards.get(&id).map_err(|error| {
        if error.is::<ShardNotFoundError>() {
            tonic::Status::not_found(error.to_string())
        } else {
            tonic::Status::internal(format!("Error lazy loading shard {id}: {error:?}"))
        }
    })?;
    Ok(shard)
}

#[tonic::async_trait]
impl NodeWriter for NodeWriterGRPCDriver {
    async fn new_shard(&self, request: Request<NewShardRequest>) -> Result<Response<ShardCreated>, Status> {
        send_analytics_event(AnalyticsEvent::Create).await;
        let request = request.into_inner();
        let kbid = request.kbid.clone();
        let shard_id = uuid::Uuid::new_v4().to_string();

        #[allow(deprecated)]
        let vector_configs = if !request.vectorsets_configs.is_empty() {
            // create shard maybe with multiple vectorsets
            let mut configs = HashMap::with_capacity(request.vectorsets_configs.len());
            for (vectorset_id, config) in request.vectorsets_configs {
                configs.insert(
                    vectorset_id,
                    VectorConfig::try_from(config).map_err(|e| tonic::Status::internal(e.to_string()))?,
                );
            }
            configs
        } else if let Some(vector_index_config) = request.config {
            // DEPRECATED bw/c code, remove when shard creators populate
            // vector_index_configs
            HashMap::from([(
                DEFAULT_VECTORS_INDEX_NAME.to_string(),
                VectorConfig::try_from(vector_index_config).map_err(|e| tonic::Status::internal(e.to_string()))?,
            )])
        } else {
            // DEPRECATED bw/c code, remove when shard creators populate
            // vector_index_configs
            HashMap::from([(
                DEFAULT_VECTORS_INDEX_NAME.to_string(),
                VectorConfig {
                    similarity: request.similarity().into(),
                    normalize_vectors: request.normalize_vectors,
                    ..Default::default()
                },
            )])
        };

        let shards = Arc::clone(&self.shards);
        let new_shard = tokio::task::spawn_blocking(move || {
            shards.create(NewShard {
                kbid,
                shard_id,
                vector_configs,
            })
        })
        .await
        .map_err(|error| tonic::Status::internal(format!("Error creating shard: {error:?}")))?;

        match new_shard {
            Ok(new_shard) => {
                self.emit_event(NodeWriterEvent::ShardCreation);
                Ok(tonic::Response::new(ShardCreated {
                    id: new_shard.id.clone(),
                    document_service: new_shard.document_version() as i32,
                    paragraph_service: new_shard.paragraph_version() as i32,
                    vector_service: new_shard.vector_version() as i32,
                    relation_service: new_shard.relation_version() as i32,
                }))
            }
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn delete_shard(&self, request: Request<ShardId>) -> Result<Response<ShardId>, Status> {
        send_analytics_event(AnalyticsEvent::Delete).await;
        // Deletion does not require for the shard to be loaded.
        let shard_id = request.into_inner();

        let shards = Arc::clone(&self.shards);
        let shard_id_clone = shard_id.id.clone();
        let deleted = tokio::task::spawn_blocking(move || shards.delete(&shard_id_clone))
            .await
            .map_err(|error| tonic::Status::internal(format!("Error deleted shard {}: {error:?}", shard_id.id)))?;

        match deleted {
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

    async fn list_shards(&self, _request: Request<EmptyQuery>) -> Result<Response<ShardIds>, Status> {
        let shard_ids = list_shards(self.shards.shards_path.clone())
            .await
            .into_iter()
            .map(|id| ShardId {
                id,
            })
            .collect();

        Ok(tonic::Response::new(ShardIds {
            ids: shard_ids,
        }))
    }

    async fn set_resource(&self, request: Request<Resource>) -> Result<Response<OpStatus>, Status> {
        let span = Span::current();
        let resource = request.into_inner();
        let shard_id = resource.shard_id.clone();
        let shards = Arc::clone(&self.shards);
        let shard_id_clone = shard_id.clone();
        let info = info_span!(parent: &span, "set resource");
        let write_task = || {
            run_with_telemetry(info, move || {
                let shard = obtain_shard(shards, shard_id_clone)?;
                shard.set_resource(resource)
            })
        };
        let result = tokio::task::spawn_blocking(write_task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok as i32,
                detail: "Success!".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error as i32,
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(status))
    }

    async fn set_resource_from_storage(&self, request: Request<IndexMessage>) -> Result<Response<OpStatus>, Status> {
        let download_start = std::time::Instant::now();

        let index_message = request.into_inner();
        let shard_id = index_message.shard.clone();
        let storage_key = index_message.storage_key.clone();

        let get_result = self.settings.object_store.get(&Path::from(storage_key)).await.map_err(|e| {
            error!("Failed to get indexing resource from object store: {}", e);
            tonic::Status::internal(format!("Failed to get indexing resource from object store: {}", e))
        })?;

        let bytes = get_result.bytes().await.map_err(|e| {
            error!("Failed to download indexing resource from object store: {}", e);
            tonic::Status::internal(format!("Failed to download indexing resource from object store: {}", e))
        })?;

        get_metrics().indexing_resource_download_histogram.observe(download_start.elapsed().as_secs_f64());

        let handle = tokio::task::spawn_blocking(move || Resource::decode(bytes)).await.unwrap();
        let mut resource = handle.map_err(|e| {
            error!("Failed to decode indexing resource: {}", e);
            tonic::Status::internal(format!("Failed to decode indexing resource: {}", e))
        })?;

        // Set the shard id to the one provided by index message
        resource.shard_id = shard_id.clone();
        let set_resource_request = Request::new(resource);
        self.set_resource(set_resource_request).await
    }

    async fn remove_resource(&self, request: Request<ResourceId>) -> Result<Response<OpStatus>, Status> {
        let span = Span::current();
        let resource = request.into_inner();
        let shard_id = resource.shard_id.clone();
        let shards = Arc::clone(&self.shards);
        let shard_id_clone = shard_id.clone();
        let info = info_span!(parent: &span, "remove resource");
        let write_task = || {
            run_with_telemetry(info, move || {
                let shard = obtain_shard(shards, shard_id_clone)?;
                shard.remove_resource(&resource)
            })
        };
        let result = tokio::task::spawn_blocking(write_task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok as i32,
                detail: "Success!".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error as i32,
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(status))
    }

    async fn add_vector_set(&self, request: Request<NewVectorSetRequest>) -> Result<Response<OpStatus>, Status> {
        let span = Span::current();

        let request = request.into_inner();
        let config = if let Some(req_config) = request.config {
            VectorConfig::try_from(req_config).map_err(|e| tonic::Status::internal(e.to_string()))?
        } else {
            #[allow(deprecated)]
            VectorConfig {
                similarity: request.similarity().into(),
                normalize_vectors: request.normalize_vectors,
                ..Default::default()
            }
        };
        let Some(VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset,
        }) = request.id
        else {
            return Ok(tonic::Response::new(OpStatus {
                status: op_status::Status::Error.into(),
                detail: "Vectorset ID must be provided".to_string(),
                ..Default::default()
            }));
        };

        let shards = Arc::clone(&self.shards);
        let task = move || {
            run_with_telemetry(info_span!(parent: &span, "Add a vectorset"), move || {
                let shard = obtain_shard(shards, shard_id.clone())?;
                shard.create_vectors_index(vectorset, config)
            })
        };
        let result = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok.into(),
                detail: "Vectorset successfully created".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error.into(),
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(status))
    }

    async fn remove_vector_set(&self, request: Request<VectorSetId>) -> Result<Response<OpStatus>, Status> {
        let span = Span::current();

        let VectorSetId {
            shard: Some(ShardId {
                id: shard_id,
            }),
            vectorset,
        } = request.into_inner()
        else {
            return Ok(tonic::Response::new(OpStatus {
                status: op_status::Status::Error.into(),
                detail: "Vectorset ID must be provided".to_string(),
                ..Default::default()
            }));
        };

        let shards = Arc::clone(&self.shards);
        let task = move || {
            run_with_telemetry(info_span!(parent: &span, "Remove vectorset"), move || {
                let shard = obtain_shard(shards, shard_id.clone())?;
                shard.remove_vectors_index(vectorset)
            })
        };
        let result = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        let status = match result {
            Ok(()) => OpStatus {
                status: op_status::Status::Ok.into(),
                detail: "Vectorset successfully deleted".to_string(),
                ..Default::default()
            },
            Err(error) => OpStatus {
                status: op_status::Status::Error.into(),
                detail: error.to_string(),
                ..Default::default()
            },
        };
        Ok(tonic::Response::new(status))
    }

    async fn list_vector_sets(&self, request: Request<ShardId>) -> Result<Response<VectorSetList>, Status> {
        let span = Span::current();

        let shard_id = request.into_inner().id;
        let shard_id_clone = shard_id.clone();
        let shards = Arc::clone(&self.shards);
        let task = move || {
            run_with_telemetry(info_span!(parent: &span, "Remove vectorset"), move || {
                let shard = obtain_shard(shards, shard_id_clone)?;
                Ok(shard.list_vectors_indexes())
            })
        };
        let result: NodeResult<Vec<String>> = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match result {
            Ok(vectorsets) => Ok(tonic::Response::new(VectorSetList {
                shard: Some(ShardId {
                    id: shard_id,
                }),
                vectorsets,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn get_metadata(&self, _request: Request<EmptyQuery>) -> Result<Response<NodeMetadata>, Status> {
        let settings = &self.settings;
        let mut total_disk = 0;
        let mut available_disk = 0;

        for disk in sysinfo::Disks::new_with_refreshed_list().into_iter() {
            total_disk += disk.total_space();
            available_disk += disk.available_space();
        }
        Ok(tonic::Response::new(NodeMetadata {
            shard_count: list_shards(settings.shards_path()).await.len().try_into().unwrap(),
            node_id: read_host_key(&settings.host_key_path).unwrap().to_string(),
            primary_node_id: get_primary_node_id(&settings.data_path),
            available_disk,
            total_disk,
            ..Default::default()
        }))
    }

    async fn gc(&self, request: Request<ShardId>) -> Result<Response<GarbageCollectorResponse>, Status> {
        send_analytics_event(AnalyticsEvent::GarbageCollect).await;
        let shard_id = request.into_inner();
        let shards = Arc::clone(&self.shards);
        let shard_id_clone = shard_id.id.clone();
        let span = Span::current();
        let info = info_span!(parent: &span, "garbage collection");
        let task = || {
            let shard = obtain_shard(shards, shard_id_clone)?;
            run_with_telemetry(info, move || shard.force_garbage_collection())
        };
        let result = tokio::task::spawn_blocking(task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;
        match result {
            Ok(status) => Ok(tonic::Response::new(GarbageCollectorResponse {
                status: garbage_collector_response::Status::from(status) as i32,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }

    async fn merge(&self, request: Request<ShardId>) -> Result<Response<MergeResponse>, Status> {
        let shard_id = request.into_inner().id;

        // The merging task can only work with already opened shards. Before
        // sending this work we make ensure that the shard will be loaded by loading it.
        let shard_id_copy = shard_id.clone();
        let shards_tasks_copy = Arc::clone(&self.shards);
        let load_shard_task = || obtain_shard(shards_tasks_copy, shard_id_copy);
        let load_shard_result = tokio::task::spawn_blocking(load_shard_task)
            .await
            .map_err(|error| tonic::Status::internal(format!("Blocking task panicked: {error:?}")))?;

        if let Err(error) = load_shard_result {
            return Err(tonic::Status::internal(error.to_string()));
        }

        let merger = global_merger();
        let merge_request = MergeRequest {
            shard_id,
            priority: MergePriority::High,
            waiter: MergeWaiter::Async,
        };
        let handle = merger.schedule(merge_request);
        let result = handle.wait().await;

        match result {
            Ok(metrics) => Ok(tonic::Response::new(MergeResponse {
                status: merge_response::MergeStatus::Ok.into(),
                merged_segments: metrics.merged as u32,
                remaining_segments: metrics.left as u32,
            })),
            Err(error) => Err(tonic::Status::internal(error.to_string())),
        }
    }
}
