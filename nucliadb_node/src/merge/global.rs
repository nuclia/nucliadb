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

use std::sync::OnceLock;

use lazy_static::lazy_static;
use nucliadb_core::merge::MergerError;

use crate::merge::MergeScheduler;

lazy_static! {
    static ref MERGE_SCHEDULER: OnceLock<MergeScheduler> = OnceLock::new();
}

/// Install merger as the global merge scheduler.
pub fn install_global(merger: MergeScheduler) -> Result<impl FnOnce(), MergerError> {
    if MERGE_SCHEDULER.get().is_some() {
        return Err(MergerError::GlobalMergerAlreadyInstalled);
    }
    let global_merger = MERGE_SCHEDULER.get_or_init(move || merger);
    Ok(move || global_merger.run_forever())
}

/// Get a referente to the global merge scheduler
///
/// This function panics if the global merger hasn't been installed
pub fn global_merger() -> &'static MergeScheduler {
    MERGE_SCHEDULER.get().expect("Global merge scheduler must be installed")
}

/// Stop global merger
pub fn stop_global_merger() {
    global_merger().stop()
}
