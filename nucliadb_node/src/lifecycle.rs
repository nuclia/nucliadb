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

//! Application initialization and finalization utilities

use std::sync::Arc;

use crate::merge::errors::MergerError;
use crate::merge::{self, MergeScheduler};
use crate::settings::Settings;
use crate::shards::providers::shard_cache::ShardWriterCache;
use nucliadb_core::prelude::*;
use nucliadb_core::thread::ThreadPoolBuilder;

/// Initialize the index node writer. This function must be called before using
/// a writer
pub fn initialize_writer(settings: Settings) -> NodeResult<()> {
    let data_path = settings.data_path();
    let shards_path = settings.shards_path();
    if !data_path.exists() {
        return Err(node_error!("Data directory ({:?}) should be already created", data_path));
    }

    if !shards_path.exists() {
        std::fs::create_dir(shards_path)?;
    }

    // We shallow the error if the threadpools were already initialized
    let _ = ThreadPoolBuilder::new().num_threads(settings.num_global_rayon_threads()).build_global();

    Ok(())
}

/// Initialize the global merge scheduler. This function must be called if merge
/// scheduler should run
pub fn initialize_merger(shard_cache: Arc<ShardWriterCache>, settings: Settings) -> Result<(), MergerError> {
    let merger = MergeScheduler::new(shard_cache, settings);
    let _ = merge::install_global(merger).map(std::thread::spawn)?;
    Ok(())
}

/// Finalizes the global merge scheduler. This function should be called before
/// finishing the process that started the merge
pub fn finalize_merger() {
    merge::stop_global_merger();
}

/// Initialize the index node reader. This function must be called before using
/// a reader
pub fn initialize_reader(settings: Settings) {
    // We swallow the error if the threadpool was already initialized
    let _ = ThreadPoolBuilder::new().num_threads(settings.num_global_rayon_threads()).build_global();
}
