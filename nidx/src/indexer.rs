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
use anyhow;
use async_nats::jetstream::consumer::PullConsumer;
use futures::stream::StreamExt;
use nucliadb_core::protos::Resource;
use nucliadb_core::protos::{prost::Message, IndexMessage};
use object_store::{DynObjectStore, ObjectStore};
use std::sync::Arc;
use tempfile::TempDir;
use tokio_util::io::SyncIoBridge;
use uuid::Uuid;

pub async fn run() -> anyhow::Result<()> {
    let settings = Settings::from_env();
    let indexer_settings = settings.indexer.expect("Indexer not configured");
    let indexer_storage = indexer_settings.object_store.client();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;

    let client = async_nats::connect(indexer_settings.nats_server).await?;
    let jetstream = async_nats::jetstream::new(client);
    let consumer: PullConsumer = jetstream.get_consumer_from_stream("nidx", "nidx").await?;
    let mut msg_stream = consumer.messages().await?;

    while let Some(Ok(msg)) = msg_stream.next().await {
        let seq = msg.info().unwrap().stream_sequence as i64;
        let body = msg.message.payload.clone();
        let index_message = IndexMessage::decode(body)?;

        let get_result = indexer_storage.get(&object_store::path::Path::from(index_message.storage_key)).await?;
        let bytes = get_result.bytes().await?;
        let resource = Resource::decode(bytes)?;

        index_resource(&meta, indexer_storage.clone(), &resource, seq).await?;
        msg.ack().await.unwrap();
    }

    Ok(())
}

async fn index_resource(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    resource: &Resource,
    seq: i64,
) -> anyhow::Result<()> {
    let shard_id = Uuid::parse_str(&resource.shard_id)?;
    println!("Indexing for shard {shard_id}");
    let shard = Shard::get(&meta, shard_id).await?;

    let indexes = shard.indexes(meta).await?;
    for index in indexes {
        let (dir, records, deletions) = index_resource_to_index(&index, resource).await?;
        if records == 0 {
            continue;
        }

        let segment = Segment::create(&meta, index.id, seq).await?;
        let store_path = format!("segment/{}", segment.id);

        let size = pack_and_upload(storage.clone(), dir, &store_path).await?;
        let mut tx = meta.transaction().await?;
        segment.mark_ready(&mut *tx, records, size).await?;
        Deletion::create(&mut *tx, index.id, seq, &deletions).await?;
        tx.commit().await?;
    }
    Ok(())
}

async fn index_resource_to_index<'a>(
    index: &Index,
    resource: &'a Resource,
) -> anyhow::Result<(TempDir, i64, &'a Vec<String>)> {
    let output_dir = tempfile::tempdir()?;
    let indexer = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer::new(),
        _ => unimplemented!(),
    };
    let (records, deletions) = indexer.index_resource(output_dir.path(), resource)?;
    Ok((output_dir, records, deletions))
}

pub struct WriteCounter<T> {
    writer: SyncIoBridge<T>,
    counter: usize,
}
impl<T> std::io::Write for WriteCounter<T>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes = self.writer.write(buf)?;
        self.counter += bytes;
        Ok(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}
impl<T> WriteCounter<T>
where
    T: tokio::io::AsyncWrite + Unpin,
{
    pub fn new(writer: T) -> Self {
        let writer = SyncIoBridge::new(writer);
        Self {
            writer,
            counter: 0,
        }
    }
    pub fn finish(&mut self) -> std::io::Result<usize> {
        self.writer.shutdown()?;
        Ok(self.counter)
    }
}

async fn pack_and_upload(storage: Arc<DynObjectStore>, dir: TempDir, store_path: &str) -> anyhow::Result<i64> {
    let mut upload = WriteCounter::new(object_store::buffered::BufWriter::new(storage, store_path.into()));
    let size = tokio::task::spawn_blocking(move || -> anyhow::Result<i64> {
        let mut tar = tar::Builder::new(&mut upload);
        tar.mode(tar::HeaderMode::Deterministic);
        tar.append_dir_all(".", dir.path())?;
        tar.finish()?;
        drop(tar);
        let bytes = upload.finish()?;
        Ok(bytes as i64)
    })
    .await??;

    Ok(size)
}

#[cfg(test)]
mod tests {

    use std::fs::File;
    use std::io::{Seek, Write};

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
        index_resource(&meta, storage.clone(), &little_prince(shard.id.to_string()), 123).await.unwrap();

        let segments = index.segments(&meta).await.unwrap();
        assert_eq!(segments.len(), 1);

        let segment = &segments[0];
        assert_eq!(segment.delete_at, None);
        assert_eq!(segment.records, Some(1));

        let download =
            storage.get(&object_store::path::Path::parse(format!("segment/{}", segment.id)).unwrap()).await.unwrap();
        let mut out = File::create("/tmp/output").unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
        let downloaded_size = out.seek(std::io::SeekFrom::Current(0)).unwrap() as i64;
        assert_eq!(downloaded_size, segment.size_bytes.unwrap());
    }
}
