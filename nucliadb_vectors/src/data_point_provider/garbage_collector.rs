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

use crate::data_point::{DataPoint, DataPointPin, DpId};
use crate::data_point_provider::VectorR;
use nucliadb_core::tracing::*;
use std::path::Path;

pub struct Metrics {
    pub garbage_not_deleted: usize,
    pub garbage_found: usize,
    pub total: usize,
}

pub fn collect_garbage(path: &Path) -> VectorR<Metrics> {
    let mut metrics = Metrics {
        garbage_not_deleted: 0,
        garbage_found: 0,
        total: 0,
    };

    for dir_entry in std::fs::read_dir(path)? {
        let entry = dir_entry?;
        let dir_path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if dir_path.is_file() {
            continue;
        }

        let Ok(data_point_id) = DpId::parse_str(&name) else {
            info!("Unknown item {path:?} found");
            continue;
        };

        metrics.total += 1;

        let Ok(is_pinned) = DataPointPin::is_pinned(path, data_point_id) else {
            warn!("Error checking {data_point_id}");
            continue;
        };

        if is_pinned {
            continue;
        }

        metrics.garbage_found += 1;
        let Err(err) = DataPoint::delete(path, data_point_id) else {
            continue;
        };

        metrics.garbage_not_deleted += 1;
        warn!("{name} is garbage and could not be deleted because of {err}");
    }

    Ok(metrics)
}
