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

use std::{
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

use anyhow::anyhow;
use futures::TryStreamExt;
use object_store::DynObjectStore;
use serde::{Deserialize, Serialize};
use sqlx::postgres::PgPoolCopyExt;
use tar::Header;
use tokio::{io::AsyncWriteExt, runtime::Handle};
use tokio_stream::StreamExt;
use tokio_util::{compat::FuturesAsyncReadCompatExt, io::SyncIoBridge};
use uuid::Uuid;

use crate::{
    metadata::{Index, IndexId, Segment},
    NidxMetadata,
};

/// Some metadata summary that is useful during import
#[derive(Debug, Serialize, Deserialize)]
struct ExportMetadata {
    shard: Uuid,
    indexes: Vec<IndexId>,
    segment_sizes: Vec<i64>,
}

pub async fn export_shard(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    shard_id: Uuid,
    index_id: Option<IndexId>,
    writer: impl Write + Send + 'static,
) -> anyhow::Result<()> {
    let indexes = if let Some(index_id) = index_id {
        let index = Index::get(&meta.pool, index_id).await?;
        if index.shard_id != shard_id {
            return Err(anyhow!("index_id does not match shard_id in download_shard()"));
        }
        vec![index]
    } else {
        Index::for_shard(&meta.pool, shard_id).await?
    };

    // Once the tar builder is created, we must be careful handling errors so that the
    // if an error occurs we don't write any additional data to `writer` as that can trigger
    // a sync write from a non-blocking tokio thread
    // This also means that this function is not safe if operating on a Write that has
    // a Drop implementation, e.g: BufWriter
    // Using zstd compression since we depend on it anyway (via Tantivy)
    let mut tar = tar::Builder::new(zstd::stream::Encoder::new(writer, 0)?);

    // Export metadata (in the order it needs to be imported)
    let index_ids: Vec<IndexId> = indexes.iter().map(|i| i.id).collect();
    let segments = Segment::in_indexes(&meta.pool, &index_ids).await?;

    let metadata = ExportMetadata {
        shard: shard_id,
        indexes: index_ids.clone(),
        segment_sizes: segments.iter().map(|s| s.size_bytes.unwrap()).collect(),
    };
    let json = serde_json::to_string(&metadata)?;
    tar = tokio::task::spawn_blocking(move || {
        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(json.len() as u64);
        tar.append_data(&mut header, "_meta/export.json", json.as_bytes())?;
        Ok::<_, anyhow::Error>(tar)
    })
    .await??;

    let index_id_str = index_ids.iter().map(|i| i.sql().to_string()).collect::<Vec<_>>().join(",");

    let query = &format!("SELECT * FROM shards WHERE id = '{shard_id}'");
    tar = archive_query(tar, &meta, query, "shards").await?;

    let query = &format!("SELECT * FROM indexes WHERE id IN ({index_id_str})");
    tar = archive_query(tar, &meta, query, "indexes").await?;

    let query = &format!("SELECT * FROM segments WHERE index_id IN ({index_id_str})");
    tar = archive_query(tar, &meta, query, "segments").await?;

    let query = &format!("SELECT * FROM deletions WHERE index_id IN ({index_id_str})");
    tar = archive_query(tar, &meta, query, "deletions").await?;

    // Export segments
    for segment in segments {
        let download = storage.get(&segment.id.storage_key()).await?;
        let reader = download.into_stream().map_err(std::io::Error::from).into_async_read().compat();
        let sync_reader = SyncIoBridge::new(reader);

        tar = tokio::task::spawn_blocking(move || {
            let mut header = Header::new_gnu();
            header.set_mode(0o644);
            header.set_size(segment.size_bytes.unwrap() as u64);
            tar.append_data(&mut header, segment.id.storage_key().to_string(), sync_reader)?;
            Ok::<_, anyhow::Error>(tar)
        })
        .await??;
    }

    tokio::task::spawn_blocking(move || tar.into_inner()?.finish()).await??;

    Ok(())
}

async fn archive_query<W>(
    mut tar: tar::Builder<W>,
    meta: &NidxMetadata,
    query: &str,
    filename: &str,
) -> anyhow::Result<tar::Builder<W>>
where
    W: Write + Send + 'static,
{
    let mut query = meta.pool.copy_out_raw(&format!("COPY ({query}) TO STDOUT")).await?;

    // Read all the data to a buffer since we need to know the length beforehand
    let mut data = Vec::new();
    while let Some(chunk) = query.next().await {
        data.extend_from_slice(&chunk?);
    }
    let path = format!("_meta/{filename}");
    tokio::task::spawn_blocking(move || {
        let mut header = Header::new_gnu();
        header.set_mode(0o644);
        header.set_size(data.len() as u64);
        tar.append_data(&mut header, &path, &data[..])?;
        Ok::<_, anyhow::Error>(tar)
    })
    .await?
}

pub async fn import_shard(
    meta: NidxMetadata,
    storage: Arc<DynObjectStore>,
    reader: impl Read + Send + 'static,
) -> anyhow::Result<()> {
    let mut metadata = None;
    let mut segment_count = 0;
    tokio::task::spawn_blocking(move || {
        let mut tar = tar::Archive::new(zstd::stream::Decoder::new(reader)?);

        for entry in tar.entries()? {
            let mut entry = entry?;
            let path = entry.path()?.into_owned();
            if let Ok(table) = path.strip_prefix("_meta/") {
                if table.as_os_str() == "export.json" {
                    let export_meta: ExportMetadata = serde_json::from_reader(entry)?;
                    println!(
                        "Importing shard {} ({} indexes, {} segments, {} MB)",
                        export_meta.shard,
                        export_meta.indexes.len(),
                        export_meta.segment_sizes.len(),
                        export_meta.segment_sizes.iter().sum::<i64>() / 1_000_000
                    );
                    metadata = Some(export_meta);
                } else {
                    Handle::current().block_on(import_sql(&meta, table.to_str().unwrap(), &mut entry))?;
                }
            } else {
                Handle::current().block_on(import_file(&storage, path, &mut entry))?;
                segment_count += 1;
            }

            // Progress tracking
            if let Some(meta) = &metadata {
                let mut downloaded_bytes = 0;
                let mut total_bytes = 0;
                for (i, b) in meta.segment_sizes.iter().enumerate() {
                    if i < segment_count {
                        downloaded_bytes += b;
                    }
                    total_bytes += b;
                }
                println!(
                    "{:3.0}% Downloaded {}/{} segments ({}/{} MB)",
                    100.0 * downloaded_bytes as f32 / total_bytes as f32,
                    segment_count,
                    meta.segment_sizes.len(),
                    downloaded_bytes / 1_000_000,
                    total_bytes / 1_000_000
                );
            }
        }

        Ok(())
    })
    .await?
}

async fn import_sql(meta: &NidxMetadata, table: &str, mut reader: impl Read) -> anyhow::Result<()> {
    let mut copy = meta.pool.copy_in_raw(&format!("COPY {table:?} FROM STDIN")).await?;
    let mut buf = [0; 4096];
    while let Ok(read) = reader.read(&mut buf) {
        if read == 0 {
            break;
        };
        copy.send(&buf[..read]).await?;
    }
    copy.finish().await?;

    Ok(())
}

async fn import_file(storage: &Arc<DynObjectStore>, path: PathBuf, mut reader: impl Read) -> anyhow::Result<()> {
    let mut upload =
        object_store::buffered::BufWriter::new(storage.clone(), path.into_os_string().into_string().unwrap().into());

    let mut buf = [0; 4096];
    while let Ok(read) = reader.read(&mut buf) {
        if read == 0 {
            break;
        };
        upload.write_all(&buf[..read]).await?;
    }
    upload.shutdown().await?;

    Ok(())
}
