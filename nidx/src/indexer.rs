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
    io::{Read, Seek},
    sync::Arc,
};

use crate::metadata::*;
use anyhow;
use nucliadb_core::protos::Resource;
use object_store::{DynObjectStore, ObjectStore};
use tempfile::TempDir;
use tokio::io::AsyncWriteExt;
use tokio_util::io::SyncIoBridge;

async fn index_resource(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    shard: &Shard,
    resource: &Resource,
) -> anyhow::Result<()> {
    let indexes = shard.indexes(meta).await?;
    for index in indexes {
        let dir = index_resource_to_index(&index, resource).await?;

        let segment = Segment::create(&meta, index.id).await?;
        let store_path = format!("segment/{}", segment.id);

        pack_and_upload(storage.clone(), dir, &store_path).await?;
        segment.mark_ready(meta).await?;
    }
    Ok(())
}

async fn index_resource_to_index(index: &Index, resource: &Resource) -> anyhow::Result<TempDir> {
    let output_dir = tempfile::tempdir()?;
    let indexer = match index.kind {
        IndexKind::Vector => nidx_vector::VectorIndexer::new(),
        _ => unimplemented!(),
    };
    indexer.index_resource(output_dir.path(), resource)?;

    Ok(output_dir)
}

async fn pack_and_upload(storage: Arc<DynObjectStore>, dir: TempDir, store_path: &str) -> anyhow::Result<()> {
    let mut upload = SyncIoBridge::new(object_store::buffered::BufWriter::new(storage, store_path.into()));
    tokio::task::spawn_blocking(move || -> anyhow::Result<()> {
        let mut tar = tar::Builder::new(&mut upload);
        tar.mode(tar::HeaderMode::Deterministic);
        tar.append_dir_all(".", dir.path())?;
        tar.finish()?;
        drop(tar);
        upload.shutdown()?;
        Ok(())
    })
    .await??;

    Ok(())
}

#[cfg(test)]
mod tests {

    use std::fs::File;
    use std::io::Write;

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
        index_resource(&meta, storage.clone(), &shard, &little_prince("abc")).await.unwrap();

        let segments = index.segments(&meta).await.unwrap();
        assert_eq!(segments.len(), 1);

        let segment = &segments[0];
        assert_eq!(segment.ready, true);

        let download =
            storage.get(&object_store::path::Path::parse(format!("segment/{}", segment.id)).unwrap()).await.unwrap();
        let mut out = File::create("/tmp/output").unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
    }
}
