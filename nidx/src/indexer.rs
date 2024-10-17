use crate::upload::pack_and_upload;
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
use crate::{metadata::*, Settings};
use anyhow::anyhow;
use async_nats::jetstream::consumer::PullConsumer;
use async_nats::Message;
use futures::stream::StreamExt;
use nidx_protos::prost::*;
use nidx_protos::IndexMessage;
use nidx_protos::Resource;
use nidx_types::Seq;
use object_store::{DynObjectStore, ObjectStore};
use std::path::Path;
use std::sync::Arc;
use tracing::*;
use uuid::Uuid;

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    let indexer_settings = settings.indexer.ok_or(anyhow!("Indexer settings required"))?;
    let indexer_storage = indexer_settings.object_store.client();

    let storage_settings = settings.storage.ok_or(anyhow!("Storage settings required"))?;
    let segment_storage = storage_settings.object_store.client();

    let nats_client = async_nats::connect(indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(nats_client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let mut subscription = consumer.messages().await?;

    while let Some(Ok(msg)) = subscription.next().await {
        let info = match msg.info() {
            Ok(info) => info,
            Err(e) => {
                error!("Invalid NATS message {e:?}, skipping");
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
        info!(?seq, "Processing indexing message");
        let (msg, acker) = msg.split();

        let resource = match download_message(indexer_storage.clone(), msg).await {
            Ok(r) => r,
            Err(e) => {
                warn!("Error downloading index message {e:?}");
                continue;
            }
        };

        match index_resource(&meta, segment_storage.clone(), &resource, seq).await {
            Ok(()) => {
                if let Err(e) = acker.ack().await {
                    warn!("Error ack'ing NATS message {e:?}")
                }
            }
            Err(e) => {
                warn!("Error processing index message {e:?}")
            }
        };
    }

    Ok(())
}

pub async fn download_message(storage: Arc<DynObjectStore>, msg: Message) -> anyhow::Result<Resource> {
    let index_message = IndexMessage::decode(msg.payload)?;

    let get_result = storage.get(&object_store::path::Path::from(index_message.storage_key)).await?;
    let bytes = get_result.bytes().await?;
    let resource = Resource::decode(bytes)?;

    Ok(resource)
}

async fn index_resource(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    resource: &Resource,
    seq: Seq,
) -> anyhow::Result<()> {
    let shard_id = Uuid::parse_str(&resource.shard_id)?;
    let indexes = Index::for_shard(meta, shard_id).await?;

    for index in indexes {
        let output_dir = tempfile::tempdir()?;
        let (records, deletions) = index_resource_to_index(&index, resource, output_dir.path()).await?;
        if records == 0 {
            continue;
        }

        // Create the segment first so we can track it if the upload gets interrupted
        let segment = Segment::create(meta, index.id, seq).await?;
        let size = pack_and_upload(storage.clone(), output_dir.path(), segment.id.storage_key()).await?;

        // Mark the segment as visible and write the deletions at the same time
        let mut tx = meta.transaction().await?;
        segment.mark_ready(&mut *tx, records, size as i64).await?;
        Deletion::create(&mut *tx, index.id, seq, &deletions).await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn index_resource_to_index(
    index: &Index,
    resource: &Resource,
    output_dir: &Path,
) -> anyhow::Result<(i64, Vec<String>)> {
    let (records, deletions) = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer.index_resource(output_dir, resource)?,
        _ => unimplemented!(),
    };

    Ok((records, deletions))
}

#[cfg(test)]
mod tests {
    use std::io::{Seek, Write};

    use tempfile::tempfile;
    use uuid::Uuid;

    use super::*;
    use crate::metadata::{IndexKind, NidxMetadata};
    use crate::test::*;

    #[sqlx::test]
    async fn test_index_resource(pool: sqlx::PgPool) {
        let meta = NidxMetadata::new_with_pool(pool).await.unwrap();
        let kbid = Uuid::new_v4();
        let shard = Shard::create(&meta, kbid).await.unwrap();
        let index = Index::create(&meta, shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();

        let storage = Arc::new(object_store::memory::InMemory::new());
        index_resource(&meta, storage.clone(), &little_prince(shard.id.to_string()), 123i64.into()).await.unwrap();

        let segments = index.segments(&meta).await.unwrap();
        assert_eq!(segments.len(), 1);

        let segment = &segments[0];
        assert_eq!(segment.delete_at, None);
        assert_eq!(segment.records, Some(1));

        let download = storage.get(&object_store::path::Path::parse(segment.id.storage_key()).unwrap()).await.unwrap();
        let mut out = tempfile().unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
        let downloaded_size = out.stream_position().unwrap() as i64;
        assert_eq!(downloaded_size, segment.size_bytes.unwrap());
    }
}
