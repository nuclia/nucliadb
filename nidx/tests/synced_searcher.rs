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

use std::sync::Arc;
use std::time::Duration;

use nidx::indexer::index_resource;
use nidx::metadata::Deletion;
use nidx::searcher::SyncedSearcher;
use nidx::{
    metadata::{Index, Shard},
    NidxMetadata,
};
use nidx_protos::VectorSearchRequest;
use nidx_tests::*;
use nidx_vector::config::VectorConfig;
use nidx_vector::VectorSearcher;
use tempfile::tempdir;

#[sqlx::test]
async fn test_synchronization(pool: sqlx::PgPool) -> anyhow::Result<()> {
    let meta = NidxMetadata::new_with_pool(pool).await?;
    let storage = Arc::new(object_store::memory::InMemory::new());
    let work_dir = tempdir()?;

    let synced_searcher = SyncedSearcher::new(meta.clone(), work_dir.path());
    let index_cache = synced_searcher.index_cache();
    let storage_copy = storage.clone();
    let search_task = tokio::spawn(async move { synced_searcher.run(storage_copy).await });

    let index = Index::create(
        &meta.pool,
        Shard::create(&meta.pool, uuid::Uuid::new_v4()).await?.id,
        "english",
        VectorConfig::default().into(),
    )
    .await?;

    let search_request = VectorSearchRequest {
        vector: vec![0.5, 0.5, 0.5],
        result_per_page: 10,
        ..Default::default()
    };

    // Initially, index is not available for seach
    assert!(index_cache.get(&index.id).await.is_err());

    // Index a resource and search for it
    let resource = little_prince(index.shard_id.to_string());
    index_resource(&meta, storage.clone(), &index.shard_id.to_string(), &resource, 1i64.into()).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    // TODO: Test with shard_search once we have more indexes. We can make IndexSearch enum private again
    let searcher = index_cache.get(&index.id).await?;
    let vector_searcher: &VectorSearcher = searcher.as_ref().into();
    let result = vector_searcher.search(&search_request, &Default::default())?;
    assert_eq!(result.documents.len(), 1);

    // Delete the resource, it should disappear from results
    // TODO: Test by sending a deletion message to the deletion method (not implemented yet)
    Deletion::create(&meta.pool, index.id, 2i64.into(), &[resource.resource.unwrap().uuid]).await?;
    index.updated(&meta.pool).await?;

    tokio::time::sleep(Duration::from_secs(2)).await;

    let searcher = index_cache.get(&index.id).await?;
    let vector_searcher: &VectorSearcher = searcher.as_ref().into();
    let result = vector_searcher.search(&search_request, &Default::default())?;
    assert_eq!(result.documents.len(), 0);

    search_task.abort();

    Ok(())
}
