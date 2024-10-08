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
use std::io::{Read, Seek};

use anyhow;
use nucliadb_core::protos::Resource;
use object_store::ObjectStore;
use tempfile::{tempfile, TempDir};

use crate::metadata::{Index, IndexKind, NidxMetadata, Shard};

async fn index_resource(
    meta: &NidxMetadata,
    storage: &impl ObjectStore,
    shard: &Shard,
    resource: &Resource,
) -> anyhow::Result<()> {
    let indexes = meta.get_indexes_for_shard(shard.id).await?;
    for index in indexes {
        let dir = index_resource_to_index(&index, resource).await?;

        let segment = meta.create_segment(index.id).await?;
        let store_path = format!("{}/{}/{}/{}", shard.kbid, shard.id, index.id, segment.id);

        pack_and_upload(storage, dir, &store_path).await?;
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

async fn pack_and_upload(storage: &impl ObjectStore, dir: TempDir, store_path: &str) -> anyhow::Result<()> {
    // This is all done sync. Can be async and streaming with async_tar, but needs some adapter layers
    let mut tar_file = tempfile()?;
    let mut tar = tar::Builder::new(&mut tar_file);
    tar.append_dir_all(".", dir.path())?;
    tar.finish()?;
    drop(tar);
    drop(dir);

    let mut buf = Vec::new();
    tar_file.rewind().unwrap();
    tar_file.read_to_end(&mut buf)?;
    storage.put(&object_store::path::Path::parse(store_path)?, buf.into()).await?;

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
        let shard = meta.create_shard(kbid).await.unwrap();
        let index = meta.create_index(shard.id, IndexKind::Vector, Some("multilingual")).await.unwrap();

        let storage = object_store::memory::InMemory::new();
        index_resource(&meta, &storage, &shard, &little_prince("abc")).await.unwrap();

        let segments = meta.get_segments(index.id).await.unwrap();
        assert_eq!(segments.len(), 1);

        let segment = &segments[0];
        let download = storage
            .get(
                &object_store::path::Path::parse(format!("{}/{}/{}/{}", shard.kbid, shard.id, index.id, segment.id))
                    .unwrap(),
            )
            .await
            .unwrap();
        let mut out = File::create("/tmp/output").unwrap();
        out.write_all(&download.bytes().await.unwrap()).unwrap();
    }
}
