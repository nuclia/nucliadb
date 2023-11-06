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

use nucliadb_core::prelude::*;
use nucliadb_core::thread::ThreadPoolBuilder;
use nucliadb_vectors::data_point_provider::Merger as VectorsMerger;

use crate::settings::Settings;

/// Initialize the index node writer. This function must be called before using
/// a writer
pub fn initialize_writer(settings: Settings) -> NodeResult<()> {
    let data_path = settings.data_path();
    let shards_path = settings.shards_path();
    if !data_path.exists() {
        return Err(node_error!(
            "Data directory ({:?}) should be already created",
            data_path
        ));
    }

    if !shards_path.exists() {
        std::fs::create_dir(shards_path)?;
    }

    // We shallow the error if the threadpools were already initialized
    let _ = ThreadPoolBuilder::new()
        .num_threads(settings.num_global_rayon_threads())
        .build_global();
    let _ = VectorsMerger::install_global().map(std::thread::spawn);

    Ok(())
}

/// Initialize the index node reader. This function must be called before using
/// a reader
pub fn initialize_reader(settings: Settings) {
    // We swallow the error if the threadpool was already initialized
    let _ = ThreadPoolBuilder::new()
        .num_threads(settings.num_global_rayon_threads())
        .build_global();
}
