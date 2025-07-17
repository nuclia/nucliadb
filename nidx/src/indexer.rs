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
use anyhow::anyhow;
use async_nats::jetstream::consumer::PullConsumer;
use futures::stream::StreamExt;
use nidx_protos::IndexMessage;
use nidx_protos::OpStatus;
use nidx_protos::Resource;
use nidx_protos::TypeMessage;
use nidx_protos::nidx::nidx_indexer_server::NidxIndexer;
use nidx_protos::nidx::nidx_indexer_server::NidxIndexerServer;
use nidx_protos::prost::*;
use nidx_types::Seq;
use object_store::{DynObjectStore, ObjectStore};
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::*;
use uuid::Uuid;

use crate::errors::NidxError;
use crate::grpc_server::GrpcServer;
use crate::metrics::IndexKindLabels;
use crate::metrics::OperationStatusLabels;
use crate::metrics::indexer::*;
use crate::segment_store::pack_and_upload;
use crate::utilization_tracker::UtilizationTracker;
use crate::{Settings, metadata::*};

#[cfg(feature = "telemetry")]
use crate::telemetry;

pub async fn run(
    settings: Settings,
    shutdown: CancellationToken,
    nats_client: Option<async_nats::Client>,
) -> anyhow::Result<()> {
    settings.indexer.as_ref().ok_or(anyhow!("Indexer settings required"))?;
    if let Some(nats_client) = nats_client {
        run_nats(settings, shutdown, nats_client).await
    } else {
        let service = IndexerServer::new(settings)?.into_service();
        let server = GrpcServer::new("0.0.0.0:10002").await?;
        debug!("Running indexer grpc server at port {}", server.port()?);
        server.serve(service, shutdown).await?;

        Ok(())
    }
}

pub struct IndexerServer {
    meta: NidxMetadata,
    indexer_storage: Arc<DynObjectStore>,
    segment_storage: Arc<DynObjectStore>,
}

impl IndexerServer {
    pub fn new(settings: Settings) -> anyhow::Result<Self> {
        let meta = settings.metadata.clone();

        let indexer_settings = settings.indexer.as_ref().ok_or(anyhow!("Indexer settings required"))?;
        let indexer_storage = Arc::clone(&indexer_settings.object_store);

        let storage_settings = settings.storage.as_ref().ok_or(anyhow!("Storage settings required"))?;
        let segment_storage = Arc::clone(&storage_settings.object_store);

        Ok(Self {
            meta,
            indexer_storage,
            segment_storage,
        })
    }

    pub fn into_service(self) -> axum::Router {
        tonic::service::Routes::new(NidxIndexerServer::new(self)).into_axum_router()
    }
}

#[tonic::async_trait]
impl NidxIndexer for IndexerServer {
    async fn index(&self, request: tonic::Request<IndexMessage>) -> tonic::Result<tonic::Response<OpStatus>> {
        let msg = request.into_inner();

        let request = IndexRequest::create(&self.meta.pool).await.map_err(NidxError::from)?;
        process_index_message(
            &self.meta,
            self.indexer_storage.clone(),
            self.segment_storage.clone(),
            &tempfile::env::temp_dir(),
            msg,
            request.seq(),
        )
        .await
        .map_err(NidxError::from)?;

        request.delete(&self.meta.pool).await.map_err(NidxError::from)?;

        Ok(tonic::Response::new(OpStatus {
            status: nidx_protos::op_status::Status::Ok.into(),
            ..Default::default()
        }))
    }
}

pub async fn run_nats(
    settings: Settings,
    shutdown: CancellationToken,
    nats_client: async_nats::Client,
) -> anyhow::Result<()> {
    let meta = settings.metadata.clone();

    let indexer_settings = settings.indexer.as_ref().ok_or(anyhow!("Indexer settings required"))?;
    let indexer_storage = &indexer_settings.object_store;

    let storage_settings = settings.storage.as_ref().ok_or(anyhow!("Storage settings required"))?;
    let segment_storage = &storage_settings.object_store;

    let jetstream = async_nats::jetstream::new(nats_client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let message_ttl = consumer.cached_info().config.ack_wait;
    let in_progress_interval = message_ttl.mul_f32(0.8);
    let mut subscription = consumer.stream().max_messages_per_batch(1).messages().await?;

    let work_path = match &settings.work_path {
        Some(work_path) => PathBuf::from(work_path),
        None => tempfile::env::temp_dir(),
    };

    let utilization = UtilizationTracker::new(|busy, duration| {
        INDEXING_BUSY.get_or_create(&busy.into()).inc_by(duration.as_secs_f64());
    });

    while !shutdown.is_cancelled() {
        utilization.idle().await;
        let sub_msg = tokio::select! {
            sub_msg = subscription.next() => sub_msg,
            _ = shutdown.cancelled() => { return Ok(()) }
        };
        let msg = match sub_msg {
            Some(Ok(msg)) => msg,
            Some(Err(e)) => return Err(e.into()),
            None => return Err(anyhow!("Could not get message from NATS")),
        };

        let info = match msg.info() {
            Ok(info) => info,
            Err(e) => {
                error!("Invalid NATS message, skipping: {e:?}");
                let _ = msg.ack().await;
                continue;
            }
        };
        let seq = info.stream_sequence.into();
        if info.delivered > 5 {
            warn!(?seq, "Message exhausted retries, skipping");
            let _ = msg.ack().await;
            continue;
        }

        utilization.busy().await;
        let span = info_span!("indexer_message", ?seq);
        let (msg, acker) = msg.split();

        #[cfg(feature = "telemetry")]
        if let Some(headers) = msg.headers {
            telemetry::set_trace_from_nats(&span, headers);
        }

        let index_message = match IndexMessage::decode(msg.payload) {
            Ok(index_message) => index_message,
            Err(e) => {
                warn!("Error decoding index message: {e:?}");
                continue;
            }
        };

        // Start keepalive task to mark progress
        let (ack, mut ack_rx) = tokio::sync::oneshot::channel::<()>();
        let keepalive = tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(in_progress_interval) => {
                        if let Err(e) = acker.ack_with(async_nats::jetstream::AckKind::Progress).await {
                            warn!("Error acking message as in progress: {e:?}");
                        }
                    },
                    _ = &mut ack_rx => {
                        return acker;
                    }
                }
            }
        });

        if let Err(e) = process_index_message(
            &meta,
            indexer_storage.clone(),
            segment_storage.clone(),
            &work_path,
            index_message.clone(),
            seq,
        )
        .instrument(span)
        .await
        {
            INDEXING_COUNTER.get_or_create(&OperationStatusLabels::failure()).inc();
            error!(?seq, "Error processing index message: {e:?}");
            continue;
        }
        INDEXING_COUNTER.get_or_create(&OperationStatusLabels::success()).inc();

        // Stop keepalive task and send final ACK
        if let Err(e) = ack.send(()) {
            error!("Cannot stop keepalive task: {e:?}");
            continue;
        };
        let acker = keepalive.await?;
        if let Err(e) = acker.double_ack().await {
            warn!("Error acking index message: {e:?}");
            continue;
        }

        // Send a notification to NucliaDB: the resource has been indexed
        let notification = nidx_protos::nidx::Notification {
            kbid: index_message.kbid.clone(),
            uuid: index_message.resource.clone(),
            seqid: index_message.txid as i64,
            action: nidx_protos::nidx::notification::Action::Indexed.into(),
        };
        let channel = format!("notify.{}", notification.kbid);
        if let Err(e) = jetstream.publish(channel, notification.encode_to_vec().into()).await {
            warn!("Error sending indexed notification: {e:?}");
        }
    }

    Ok(())
}

pub async fn process_index_message(
    meta: &NidxMetadata,
    indexer_storage: Arc<DynObjectStore>,
    segment_storage: Arc<DynObjectStore>,
    work_path: &Path,
    index_message: IndexMessage,
    seq: Seq,
) -> anyhow::Result<()> {
    match index_message.typemessage() {
        TypeMessage::Deletion => delete_resource(meta, &index_message.shard, index_message.resource, seq).await,
        TypeMessage::Creation => {
            let resource = download_message(indexer_storage, &index_message.storage_key).await?;
            index_resource(meta, segment_storage, work_path, &index_message.shard, resource, seq).await
        }
    }
}

#[instrument(skip(meta))]
pub async fn delete_resource(meta: &NidxMetadata, shard_id: &str, resource: String, seq: Seq) -> anyhow::Result<()> {
    let shard_id = Uuid::parse_str(shard_id)?;
    let indexes = Index::for_shard(&meta.pool, shard_id).await?;

    let mut tx = meta.transaction().await?;
    for index in indexes {
        Deletion::create(&mut *tx, index.id, seq, &[resource.clone()]).await?;
        Index::updated(&mut *tx, &index.id).await?;
    }
    tx.commit().await?;

    Ok(())
}

#[instrument(skip(storage))]
pub async fn download_message(storage: Arc<DynObjectStore>, storage_key: &str) -> anyhow::Result<Resource> {
    let get_result = storage.get(&object_store::path::Path::from(storage_key)).await?;
    let bytes = get_result.bytes().await?;
    let resource = Resource::decode(bytes)?;

    Ok(resource)
}

type IndexingResult = (IndexId, Option<(Segment, usize)>, Vec<String>);

#[instrument(skip_all, fields(shard_id = shard_id))]
pub async fn index_resource(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    work_path: &Path,
    shard_id: &str,
    resource: Resource,
    seq: Seq,
) -> anyhow::Result<()> {
    let t = Instant::now();
    let shard_id = Uuid::parse_str(shard_id)?;
    let indexes = Index::for_shard(&meta.pool, shard_id).await?;
    let resource = Arc::new(resource);

    let rid = resource.resource.as_ref().map(|r| &r.uuid);
    info!(?seq, ?shard_id, rid, "Indexing message to shard");
    let num_vector_indexes = indexes.iter().filter(|i| matches!(i.kind, IndexKind::Vector)).count();
    let single_vector_index = num_vector_indexes == 1;

    let mut tasks: JoinSet<anyhow::Result<IndexingResult>> = JoinSet::new();
    for index in indexes {
        let resource = Arc::clone(&resource);
        let meta = meta.clone();
        let output_dir = tempfile::tempdir_in(work_path)?;
        let storage = Arc::clone(&storage);
        tasks.spawn(
            async move {
                // Index the resource
                let path = output_dir.path().to_path_buf();
                let index_2 = index.clone();
                let span = Span::current();
                let (new_segment, deletions) = tokio::task::spawn_blocking(move || {
                    span.in_scope(|| index_resource_to_index(&index_2, &resource, &path, single_vector_index))
                })
                .await??;
                let Some(new_segment) = new_segment else {
                    return Ok((index.id, None, deletions));
                };

                // Create the segment first so we can track it if the upload gets interrupted
                let segment = Segment::create(
                    &meta.pool,
                    index.id,
                    seq,
                    new_segment.records,
                    new_segment.index_metadata,
                )
                .await?;
                let size = pack_and_upload(storage.clone(), output_dir.path(), segment.id.storage_key()).await?;

                Ok((index.id, Some((segment, size)), deletions))
            }
            .in_current_span(),
        );
    }

    let results: anyhow::Result<Vec<_>> = tasks.join_all().await.into_iter().collect();

    // Commit all indexes in a single transaction. In case one fails, no changes will be made
    // and the index message can be safely reprocessed
    let mut tx = meta.transaction().await?;
    let mut indexes = Vec::new();
    for (index_id, segment_result, deletions) in results?.into_iter() {
        let mut update_index = false;
        if let Some((segment, size)) = segment_result {
            segment.mark_ready(&mut *tx, size as i64).await?;
            update_index = true;
        }
        if !deletions.is_empty() {
            Deletion::create(&mut *tx, index_id, seq, &deletions).await?;
            update_index = true;
        }
        if update_index {
            indexes.push(index_id)
        }
    }
    Index::updated_many(&mut *tx, &indexes).await?;
    tx.commit().await?;
    TOTAL_INDEXING_TIME.observe(t.elapsed().as_secs_f64());

    Ok(())
}

fn index_resource_to_index(
    index: &Index,
    resource: &Resource,
    output_dir: &Path,
    single_vector_index: bool,
) -> anyhow::Result<(Option<NewSegment>, Vec<String>)> {
    let t = Instant::now();
    let segment = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer
            .index_resource(output_dir, &index.config()?, resource, &index.name, single_vector_index)?
            .map(|x| x.into()),
        IndexKind::Text => nidx_text::TextIndexer
            .index_resource(output_dir, index.config()?, resource)?
            .map(|x| x.into()),
        IndexKind::Paragraph => nidx_paragraph::ParagraphIndexer
            .index_resource(output_dir, resource)?
            .map(|x| x.into()),
        IndexKind::Relation => nidx_relation::RelationIndexer
            .index_resource(output_dir, &index.config()?, resource)?
            .map(|x| x.into()),
    };

    let deletions = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer.deletions_for_resource(resource, &index.name),
        IndexKind::Text => nidx_text::TextIndexer.deletions_for_resource(resource),
        IndexKind::Paragraph => nidx_paragraph::ParagraphIndexer.deletions_for_resource(resource),
        IndexKind::Relation => nidx_relation::RelationIndexer.deletions_for_resource(&index.config()?, resource),
    };
    PER_INDEX_INDEXING_TIME
        .get_or_create(&IndexKindLabels::new(index.kind))
        .observe(t.elapsed().as_secs_f64());

    Ok((segment, deletions))
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};

    use nidx_protos::StringList;
    use nidx_vector::config::{Similarity, VectorCardinality, VectorConfig, VectorType};
    use tempfile::tempfile;
    use uuid::Uuid;

    use super::*;
    use crate::metadata::NidxMetadata;
    use nidx_tests::*;

    const VECTOR_CONFIG: VectorConfig = VectorConfig {
        similarity: Similarity::Cosine,
        normalize_vectors: false,
        vector_type: VectorType::DenseF32 { dimension: 3 },
        flags: vec![],
        vector_cardinality: VectorCardinality::Single,
    };

    #[sqlx::test]
    async fn test_index_resource(pool: sqlx::PgPool) {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await.unwrap();
        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into())
            .await
            .unwrap();

        let storage = Arc::new(object_store::memory::InMemory::new());
        index_resource(
            &meta,
            storage.clone(),
            &tempfile::env::temp_dir(),
            &shard.id.to_string(),
            little_prince(shard.id.to_string(), None),
            123i64.into(),
        )
        .await
        .unwrap();

        let segments = index.segments(&meta.pool).await.unwrap();
        assert_eq!(segments.len(), 1);

        let segment = &segments[0];
        assert_eq!(segment.delete_at, None);
        assert_eq!(segment.records, 1);

        let download = storage
            .get(&object_store::path::Path::parse(segment.id.storage_key()).unwrap())
            .await
            .unwrap();
        let mut out = tempfile().unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
        let downloaded_size = out.stream_position().unwrap() as i64;
        assert_eq!(downloaded_size, segment.size_bytes.unwrap());
    }

    #[sqlx::test]
    async fn test_index_deletions(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let storage = Arc::new(object_store::memory::InMemory::new());

        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

        // Resource with no deletions, no `Deletion` is created
        let mut resource = little_prince(shard.id.to_string(), None);
        index_resource(
            &meta,
            storage.clone(),
            &tempfile::env::temp_dir(),
            &shard.id.to_string(),
            resource.clone(),
            1i64.into(),
        )
        .await?;

        assert!(
            Deletion::for_index_and_seq(&meta.pool, index.id, 10i64.into())
                .await?
                .is_empty()
        );

        // Resource with some deletions, a single `Deletion` with multiple keys is created
        let keys = vec![
            format!("{}/a/title/0-15", resource.resource.as_ref().unwrap().uuid),
            format!("{}/a/summary/0-150", resource.resource.as_ref().unwrap().uuid),
        ];
        resource
            .vector_prefixes_to_delete
            .insert(index.name, StringList { items: keys.clone() });
        index_resource(
            &meta,
            storage.clone(),
            &tempfile::env::temp_dir(),
            &shard.id.to_string(),
            resource,
            2i64.into(),
        )
        .await?;

        let deletions = Deletion::for_index_and_seq(&meta.pool, index.id, 10i64.into()).await?;
        assert_eq!(deletions.len(), 1);
        assert_eq!(deletions[0].keys, keys);

        Ok(())
    }

    #[sqlx::test]
    async fn test_index_deletions_empty_segment(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let storage = Arc::new(object_store::memory::InMemory::new());

        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await?;
        let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

        // Index a resource with only deletions -- no new segment is created
        let mut resource = minimal_resource(shard.id.to_string());
        resource.texts_to_delete.push("uuid/t/title".to_string());
        resource.vector_prefixes_to_delete.insert(
            "multilingual".to_string(),
            StringList {
                items: vec!["uuid/t/title".to_string()],
            },
        );
        resource.paragraphs_to_delete.push("uuid/t/title".to_string());
        resource.relation_fields_to_delete.push("uuid/t/title".to_string());

        index_resource(
            &meta,
            storage.clone(),
            &tempfile::env::temp_dir(),
            &shard.id.to_string(),
            resource.clone(),
            1i64.into(),
        )
        .await?;

        let deletions = Deletion::for_index_and_seq(&meta.pool, index.id, 10i64.into()).await?;
        assert!(!deletions.is_empty());

        Ok(())
    }
}
