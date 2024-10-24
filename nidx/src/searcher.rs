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

mod grpc;
mod index_cache;
mod metadata;
mod shard_search;
mod sync;

use index_cache::IndexCache;
use metadata::SearchMetadata;
use sync::run_sync;

use std::sync::Arc;

use tempfile::tempdir;

use crate::{NidxMetadata, Settings};

pub async fn run() -> anyhow::Result<()> {
    let work_dir = tempdir()?;
    let settings = Settings::from_env();
    let meta = NidxMetadata::new(&settings.metadata.database_url).await?;
    let storage = settings.storage.expect("Storage settings needed").object_store.client();

    let (tx, mut rx) = tokio::sync::mpsc::channel(1000);
    let index_metadata = Arc::new(SearchMetadata::new(work_dir.path().to_path_buf(), tx));
    let index_cache = Arc::new(IndexCache::new(index_metadata.clone(), meta.clone()));

    let sync_task = tokio::task::spawn(run_sync(meta.clone(), storage.clone(), index_metadata.clone()));

    let index_cache_copy = index_cache.clone();
    let refresher_task = tokio::task::spawn(async move {
        while let Some(index_id) = rx.recv().await {
            // TODO: Do something smarter
            index_cache_copy.remove(&index_id).await;
        }
    });

    let api = grpc::SearchServer::new(meta.clone(), index_cache);
    let api_task = tokio::task::spawn(api.serve());

    tokio::select! {
        r = sync_task => {
            println!("sync_task() completed first {:?}", r)
        }
        r = api_task => {
            println!("api_task() completed first {:?}", r)
        }
        r = refresher_task => {
            println!("refresher_task() completed first {:?}", r)
        }
    }

    Ok(())
}
