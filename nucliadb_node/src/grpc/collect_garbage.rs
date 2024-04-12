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

use crate::cache::ShardWriterCache;
use nucliadb_core::tracing::*;
use std::fs::read_dir;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

fn collect_garbage(shard_id: String, cache: Arc<ShardWriterCache>) {
    let Some(shard) = cache.peek(&shard_id) else {
        return;
    };
    if let Err(err) = shard.collect_garbage() {
        error!("Garbage could not be collected from {shard_id} : {err:?}");
    }
}

pub struct GCParameters {
    pub shards_path: PathBuf,
    pub loop_interval: Duration,
}

pub async fn garbage_collection_loop(parameters: GCParameters, cache: Arc<ShardWriterCache>) {
    loop {
        tokio::time::sleep(parameters.loop_interval).await;

        let shards_dir_iterator = match read_dir(&parameters.shards_path) {
            Ok(iterator) => iterator,
            Err(error) => {
                error!("Garbage collector loop can not read shards directory: {error:?}");
                continue;
            }
        };

        let mut handles = vec![];
        for entry in shards_dir_iterator {
            let Ok(entry_path) = entry.map(|entry| entry.path()) else {
                continue;
            };

            if !entry_path.is_dir() {
                continue;
            }

            let Some(shard_folder) = entry_path.file_name() else {
                continue;
            };
            let Some(shard_id) = shard_folder.to_str().map(String::from) else {
                continue;
            };
            let cache_task_copy = Arc::clone(&cache);
            let handle = tokio::task::spawn_blocking(move || {
                collect_garbage(shard_id, cache_task_copy);
            });
            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await;
        }
    }
}
