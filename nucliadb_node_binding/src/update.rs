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

use nucliadb_core::tracing::*;
use nucliadb_node::cache::ShardReaderCache;
use std::fs::read_dir;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub struct UpdateParameters {
    pub shards_path: PathBuf,
    pub refresh_rate: Duration,
}

pub fn update_loop(parameters: UpdateParameters, cache: Arc<ShardReaderCache>) {
    loop {
        std::thread::sleep(parameters.refresh_rate);

        let Ok(shards_dir_iterator) = read_dir(&parameters.shards_path) else {
            error!("Updater can not read shards directory");
            break;
        };

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
            let Some(shard) = cache.peek(&shard_id) else {
                continue;
            };

            if let Err(err) = shard.update() {
                error!("Shard {shard_id} could not be updated: {err:?}");
            }
        }
    }
}
