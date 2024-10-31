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

mod common;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use object_store::memory::InMemory;
use object_store::ObjectStore;
use uuid::Uuid;

use nidx::api::shards;
use nidx::indexer::index_resource;
use nidx::maintenance::scheduler::{purge_deleted_shards_and_indexes, purge_deletions, purge_segments};
use nidx::metadata::{IndexId};
use nidx::{metadata::Shard, NidxMetadata};
use nidx_tests::*;
use nidx_vector::config::VectorConfig;

use common::metadata::{
    get_all_shards,
    get_all_deletions,
    get_all_indexes,
    get_all_merge_jobs,
    get_all_segments,
};

#[sqlx::test]
async fn test_shards_create_and_delete(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let meta = NidxMetadata::new_with_pool(pool).await?;
    let storage: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create a new shard for a KB with 2 vectorsets
    let kbid = Uuid::new_v4();
    let vector_configs = HashMap::from([
        ("multilingual".to_string(), VectorConfig::default()),
        ("english".to_string(), VectorConfig::default()),
    ]);
    let shard = shards::create_shard(&meta, kbid, vector_configs).await?;

    let indexes = shard.indexes(&meta.pool).await?;
    // TODO: update when more indexes are created
    assert_eq!(indexes.len(), 2);

    let names = indexes.iter().map(|index| index.name.as_str()).collect::<HashSet<_>>();
    let expected = HashSet::from(["multilingual", "english"]);
    assert_eq!(names, expected);

    let shard = Shard::get(&meta.pool, shard.id).await?;
    assert!(shard.deleted_at.is_none());
    for index in shard.indexes(&meta.pool).await? {
        assert!(index.deleted_at.is_none());
    }

    // Index a resource
    let resource = little_prince(shard.id.to_string());
    index_resource(&meta, storage.clone(), &shard.id.to_string(), &resource, 1i64.into()).await?;
    index_resource(&meta, storage.clone(), &shard.id.to_string(), &resource, 2i64.into()).await?;

    for index in shard.indexes(&meta.pool).await? {
        let segments = index.segments(&meta.pool).await?;
        assert!(!segments.is_empty());
    }

    // Mark shard and indexes to delete
    shards::delete_shard(&meta, shard.id).await?;

    let shard = Shard::get(&meta.pool, shard.id).await?;
    assert!(shard.deleted_at.is_some());
    for index in shard.indexes(&meta.pool).await? {
        assert!(index.deleted_at.is_some());
        for segment in index.segments(&meta.pool).await? {
            assert!(segment.delete_at.is_some());
        }

        // Update segment deletion time to validate purge
        sqlx::query!("UPDATE segments SET delete_at = NOW() WHERE index_id = $1", index.id as IndexId,)
            .execute(&meta.pool)
            .await?;
    }

    // Purge everything
    // TODO: show better when we remove everything
    purge_deletions(&meta, 100).await?;
    purge_segments(&meta, &storage).await?;
    purge_deleted_shards_and_indexes(&meta).await?;

    assert_eq!(get_all_shards(&meta.pool).await?.into_iter().count(), 0);
    assert_eq!(get_all_indexes(&meta.pool).await?.into_iter().count(), 0);
    assert_eq!(get_all_segments(&meta.pool).await?.into_iter().count(), 0);
    assert_eq!(get_all_merge_jobs(&meta.pool).await?.into_iter().count(), 0);
    assert_eq!(get_all_deletions(&meta.pool).await?.into_iter().count(), 0);

    Ok(())
}
