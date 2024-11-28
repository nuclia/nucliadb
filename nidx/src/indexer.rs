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
use nidx_protos::prost::*;
use nidx_protos::IndexMessage;
use nidx_protos::Resource;
use nidx_protos::TypeMessage;
use nidx_types::Seq;
use object_store::{DynObjectStore, ObjectStore};
use std::path::Path;
use std::sync::Arc;
use tracing::*;
use uuid::Uuid;

use crate::segment_store::pack_and_upload;
use crate::{metadata::*, Settings};

pub async fn run(settings: Settings) -> anyhow::Result<()> {
    let meta = settings.metadata.clone();

    let indexer_settings = settings.indexer.as_ref().ok_or(anyhow!("Indexer settings required"))?;
    let indexer_storage = &indexer_settings.object_store;

    let storage_settings = settings.storage.as_ref().ok_or(anyhow!("Storage settings required"))?;
    let segment_storage = &storage_settings.object_store;

    let nats_client = async_nats::connect(&indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(nats_client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let mut subscription = consumer.stream().max_messages_per_batch(1).messages().await?;

    while let Some(Ok(msg)) = subscription.next().await {
        let info = match msg.info() {
            Ok(info) => info,
            Err(e) => {
                error!(?e, "Invalid NATS message, skipping");
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

        let (msg, acker) = msg.split();

        let index_message = match IndexMessage::decode(msg.payload) {
            Ok(index_message) => index_message,
            Err(e) => {
                warn!(?e, "Error decoding index message");
                continue;
            }
        };

        if let Err(e) =
            process_index_message(&meta, indexer_storage.clone(), segment_storage.clone(), index_message, seq).await
        {
            warn!(?e, "Error processing index message");
            continue;
        }

        if let Err(e) = acker.ack().await {
            warn!(?e, "Error acking index message");
            continue;
        }

        // TODO: Delete indexer message on success
    }

    Ok(())
}

pub async fn process_index_message(
    meta: &NidxMetadata,
    indexer_storage: Arc<DynObjectStore>,
    segment_storage: Arc<DynObjectStore>,
    index_message: IndexMessage,
    seq: Seq,
) -> anyhow::Result<()> {
    match index_message.typemessage() {
        TypeMessage::Deletion => delete_resource(meta, &index_message.shard, index_message.resource, seq).await,
        TypeMessage::Creation => {
            let resource = download_message(indexer_storage, &index_message.storage_key).await?;
            index_resource(meta, segment_storage, &index_message.shard, resource, seq).await
        }
    }
}

pub async fn delete_resource(meta: &NidxMetadata, shard_id: &str, resource: String, seq: Seq) -> anyhow::Result<()> {
    let shard_id = Uuid::parse_str(shard_id)?;
    let indexes = Index::for_shard(&meta.pool, shard_id).await?;

    let mut tx = meta.transaction().await?;
    for index in indexes {
        Deletion::create(&mut *tx, index.id, seq, &[resource.clone()]).await?;
        index.updated(&mut *tx).await?;
    }
    tx.commit().await?;

    Ok(())
}

pub async fn download_message(storage: Arc<DynObjectStore>, storage_key: &str) -> anyhow::Result<Resource> {
    let get_result = storage.get(&object_store::path::Path::from(storage_key)).await?;
    let bytes = get_result.bytes().await?;
    let resource = Resource::decode(bytes)?;

    Ok(resource)
}

pub async fn index_resource(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    shard_id: &str,
    resource: Resource,
    seq: Seq,
) -> anyhow::Result<()> {
    let shard_id = Uuid::parse_str(shard_id)?;
    let indexes = Index::for_shard(&meta.pool, shard_id).await?;
    let resource = Arc::new(resource);

    info!(?seq, ?shard_id, "Indexing message to shard");

    let num_vector_indexes = indexes.iter().filter(|i| matches!(i.kind, IndexKind::Vector)).count();
    let single_vector_index = num_vector_indexes == 1;

    // TODO: Index in parallel
    // TODO: Save all indexes as a transaction (to avoid issues reprocessing the same message)
    for index in indexes {
        let output_dir = tempfile::tempdir()?;

        // Index the resource
        let index = Arc::new(index);
        let resource = Arc::clone(&resource);
        let path = output_dir.path().to_path_buf();
        let index_2 = Arc::clone(&index);
        let (new_segment, deletions) = tokio::task::spawn_blocking(move || {
            index_resource_to_index(&index_2, &resource, &path, single_vector_index)
        })
        .await??;
        let Some(new_segment) = new_segment else {
            continue;
        };

        // Create the segment first so we can track it if the upload gets interrupted
        let segment =
            Segment::create(&meta.pool, index.id, seq, new_segment.records, new_segment.index_metadata).await?;
        let size = pack_and_upload(storage.clone(), output_dir.path(), segment.id.storage_key()).await?;

        // Mark the segment as visible and write the deletions at the same time
        let mut tx = meta.transaction().await?;
        segment.mark_ready(&mut *tx, size as i64).await?;
        Deletion::create(&mut *tx, index.id, seq, &deletions).await?;
        index.updated(&mut *tx).await?;
        tx.commit().await?;
    }
    Ok(())
}

fn index_resource_to_index(
    index: &Index,
    resource: &Resource,
    output_dir: &Path,
    single_vector_index: bool,
) -> anyhow::Result<(Option<NewSegment>, Vec<String>)> {
    let segment = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer
            .index_resource(output_dir, &index.config()?, resource, &index.name, single_vector_index)?
            .map(|x| x.into()),
        IndexKind::Text => nidx_text::TextIndexer.index_resource(output_dir, resource)?.map(|x| x.into()),
        IndexKind::Paragraph => {
            nidx_paragraph::ParagraphIndexer.index_resource(output_dir, resource)?.map(|x| x.into())
        }
        IndexKind::Relation => nidx_relation::RelationIndexer.index_resource(output_dir, resource)?.map(|x| x.into()),
    };

    let deletions = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer.deletions_for_resource(resource),
        IndexKind::Text => nidx_text::TextIndexer.deletions_for_resource(resource),
        IndexKind::Paragraph => nidx_paragraph::ParagraphIndexer.deletions_for_resource(resource),
        IndexKind::Relation => nidx_relation::RelationIndexer.deletions_for_resource(resource),
    };

    Ok((segment, deletions))
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};

    use nidx_vector::config::VectorConfig;
    use tempfile::tempfile;
    use uuid::Uuid;

    use super::*;
    use crate::metadata::NidxMetadata;
    use nidx_tests::*;

    #[sqlx::test]
    async fn test_index_resource(pool: sqlx::PgPool) {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta.pool, kbid).await.unwrap();
        let index = Index::create(&meta.pool, shard.id, "multilingual", VectorConfig::default().into()).await.unwrap();

        let storage = Arc::new(object_store::memory::InMemory::new());
        index_resource(
            &meta,
            storage.clone(),
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

        let download = storage.get(&object_store::path::Path::parse(segment.id.storage_key()).unwrap()).await.unwrap();
        let mut out = tempfile().unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
        let downloaded_size = out.stream_position().unwrap() as i64;
        assert_eq!(downloaded_size, segment.size_bytes.unwrap());
    }
}
