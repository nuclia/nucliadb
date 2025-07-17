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

use nidx_types::Seq;
use object_store::ObjectStore;
use object_store::memory::InMemory;
use sqlx::types::JsonValue;
use uuid::Uuid;

use nidx::api::shards;
use nidx::indexer::{delete_resource, index_resource};
use nidx::metadata::{Deletion, Index, Segment};
use nidx::scheduler::{purge_deleted_shards_and_indexes, purge_deletions, purge_segments};
use nidx::{NidxMetadata, metadata::Shard};
use nidx_tests::*;
use nidx_vector::config::{VectorCardinality, VectorConfig};

use common::metadata::{count_deletions, count_indexes, count_merge_jobs, count_segments, count_shards};

const VECTOR_CONFIG: VectorConfig = VectorConfig {
    similarity: nidx_vector::config::Similarity::Cosine,
    normalize_vectors: false,
    vector_type: nidx_vector::config::VectorType::DenseF32 { dimension: 3 },
    flags: vec![],
    vector_cardinality: VectorCardinality::Single,
};

#[sqlx::test]
async fn test_shards_create_and_delete(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let meta = NidxMetadata::new_with_pool(pool).await?;
    let storage: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

    // Create a new shard for a KB with 2 vectorsets
    let kbid = Uuid::new_v4();
    let vector_configs = HashMap::from([
        ("multilingual".to_string(), VECTOR_CONFIG),
        ("english".to_string(), VECTOR_CONFIG),
    ]);
    let shard = shards::create_shard(&meta, kbid, vector_configs).await?;

    let indexes = shard.indexes(&meta.pool).await?;
    assert_eq!(indexes.len(), 5);

    let names = indexes.iter().map(|index| index.name.as_str()).collect::<HashSet<_>>();
    let expected = HashSet::from(["multilingual", "english", "text", "paragraph", "relation"]);
    assert_eq!(names, expected);

    let shard = Shard::get(&meta.pool, shard.id).await?;
    assert!(shard.deleted_at.is_none());
    for index in shard.indexes(&meta.pool).await? {
        assert!(index.deleted_at.is_none());
    }

    // Index a resource with paragraph/vectors
    let resource = little_prince(shard.id.to_string(), Some(&["multilingual", "english"]));
    index_resource(
        &meta,
        storage.clone(),
        &tempfile::env::temp_dir(),
        &shard.id.to_string(),
        resource.clone(),
        1i64.into(),
    )
    .await?;
    // Index a resource with deletions
    let resource = people_and_places(shard.id.to_string());
    index_resource(
        &meta,
        storage.clone(),
        &tempfile::env::temp_dir(),
        &shard.id.to_string(),
        resource,
        2i64.into(),
    )
    .await?;

    for index in shard.indexes(&meta.pool).await? {
        let segments = index.segments(&meta.pool).await?;
        assert!(!segments.is_empty());
    }

    // Mark shard and indexes to delete
    shards::delete_shard(&meta, shard.id).await?;

    let deleted = Shard::get(&meta.pool, shard.id).await;
    assert!(matches!(deleted, Err(sqlx::Error::RowNotFound)));
    for index in Index::marked_to_delete(&meta.pool).await? {
        let index_id = index.id;
        for segment in Segment::in_index(&meta.pool, index_id).await? {
            assert!(segment.delete_at.is_some());
        }

        // Add a new segment as if it was coming from a merge job started before
        // deletion and finished just before it. The segment has no deletion
        // mark but shouldn't retain himself, its index or shard to be purged
        let segment = Segment::create(&meta.pool, index_id, Seq::from(1000i64), 100, JsonValue::Null).await?;
        segment.mark_ready(&meta.pool, 100).await.unwrap();
    }

    // Create a shard that will survive the purge, with some segments/deletions
    let kbid = Uuid::new_v4();
    let vector_configs = HashMap::from([("english".to_string(), VECTOR_CONFIG)]);
    let surviving_shard = shards::create_shard(&meta, kbid, vector_configs).await?;
    let resource = people_and_places(surviving_shard.id.to_string());
    let rid = resource.resource.as_ref().unwrap().uuid.clone();
    index_resource(
        &meta,
        storage.clone(),
        &tempfile::env::temp_dir(),
        &surviving_shard.id.to_string(),
        resource,
        3i64.into(),
    )
    .await?;
    delete_resource(&meta, &surviving_shard.id.to_string(), rid, 4i64.into()).await?;

    // Purge everything
    purge_segments(&meta, &storage).await?;
    purge_deletions(&meta, 100).await?;
    purge_deleted_shards_and_indexes(&meta).await?;

    // All is deleted except the surviving shard
    assert_eq!(count_shards(&meta.pool).await?, 1);
    assert_eq!(Shard::list_ids(&meta.pool).await?, vec![surviving_shard.id]);

    assert_eq!(count_indexes(&meta.pool).await?, 4);
    let indexes = Index::for_shard(&meta.pool, surviving_shard.id).await?;
    assert_eq!(indexes.len(), 4);

    let mut expected_segments = 0;
    let mut expected_deletions = 0;
    for index in indexes {
        expected_segments += index.segments(&meta.pool).await?.len();
        expected_deletions += Deletion::for_index_and_seq(&meta.pool, index.id, 10i64.into())
            .await?
            .len();
    }

    // Check that only the surviving shard stuff is here
    assert_eq!(count_segments(&meta.pool).await?, expected_segments);
    assert_eq!(count_merge_jobs(&meta.pool).await?, 0);
    assert_eq!(count_deletions(&meta.pool).await?, expected_deletions);

    Ok(())
}
