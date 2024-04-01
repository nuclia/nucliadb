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

use crate::data_point::{self, DataPointPin, DpId};
use crate::data_point_provider::{OPENING_FLAG, WRITING_FLAG};
use crate::{VectorErr, VectorR};
use fs2::FileExt;
use nucliadb_core::tracing::*;
use std::fs::File;
use std::path::Path;

#[derive(Default)]
pub struct Metrics {
    pub unknown_items: usize,
    pub partial_data_points: usize,
    pub garbage_not_deleted: usize,
    pub garbage_deleted: usize,
    pub total: usize,
}

pub fn collect_garbage(path: &Path) -> VectorR<Metrics> {
    let writing_path = path.join(WRITING_FLAG);
    let writing_file = File::open(writing_path)?;

    if writing_file.try_lock_exclusive().is_ok() {
        return Err(VectorErr::NoWriterError);
    };

    let lock_path = path.join(OPENING_FLAG);
    let lock_file = File::open(lock_path)?;
    lock_file.lock_exclusive()?;

    let mut metrics = Metrics::default();
    for dir_entry in std::fs::read_dir(path)? {
        let entry = dir_entry?;
        let dir_path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();
        if dir_path.is_file() {
            continue;
        }

        let Ok(data_point_id) = DpId::parse_str(&name) else {
            info!("Unknown item {dir_path:?} found");
            metrics.unknown_items += 1;
            continue;
        };

        metrics.total += 1;

        let Ok(is_pinned) = DataPointPin::is_pinned(path, data_point_id) else {
            warn!("Error checking {data_point_id}");
            metrics.partial_data_points += 1;
            continue;
        };

        if is_pinned {
            continue;
        }

        match data_point::delete(path, data_point_id) {
            Ok(_) => metrics.garbage_deleted += 1,
            Err(err) => {
                warn!("{name} is garbage not deleted: {err}");
                metrics.garbage_not_deleted += 1;
            }
        }
    }

    Ok(metrics)
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn gc_with_some_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();

        let writing_flag = File::create(index_path.join(WRITING_FLAG)).unwrap();
        writing_flag.lock_exclusive().unwrap();

        let lock_path = index_path.join(OPENING_FLAG);
        File::create(lock_path).unwrap();

        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();
        let pin = DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert!(pin.path().is_dir());
        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 2);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_all_garbage() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();

        let writing_flag = File::create(index_path.join(WRITING_FLAG)).unwrap();
        writing_flag.lock_exclusive().unwrap();

        let lock_path = index_path.join(OPENING_FLAG);
        File::create(lock_path).unwrap();

        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();
        DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 3);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_every_data_point_pinned() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();

        let writing_flag = File::create(index_path.join(WRITING_FLAG)).unwrap();
        writing_flag.lock_exclusive().unwrap();

        let lock_path = index_path.join(OPENING_FLAG);
        File::create(lock_path).unwrap();

        let pin_0 = DataPointPin::create_pin(index_path).unwrap();
        let pin_1 = DataPointPin::create_pin(index_path).unwrap();
        let pin_2 = DataPointPin::create_pin(index_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert!(pin_0.path().is_dir());
        assert!(pin_1.path().is_dir());
        assert!(pin_2.path().is_dir());
        assert_eq!(metrics.total, 3);
        assert_eq!(metrics.garbage_deleted, 0);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }

    #[test]
    fn gc_no_data_points() {
        let workspace = tempfile::tempdir().unwrap();
        let index_path = workspace.path();

        let writing_flag = File::create(index_path.join(WRITING_FLAG)).unwrap();
        writing_flag.lock_exclusive().unwrap();

        let lock_path = index_path.join(OPENING_FLAG);
        File::create(lock_path).unwrap();

        let metrics = collect_garbage(index_path).unwrap();

        assert_eq!(metrics.total, 0);
        assert_eq!(metrics.garbage_deleted, 0);
        assert_eq!(metrics.garbage_not_deleted, 0);
    }
}
