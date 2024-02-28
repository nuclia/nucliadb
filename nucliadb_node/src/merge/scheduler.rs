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

use std::fs;
use std::sync::OnceLock;
use std::{
    collections::BinaryHeap,
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::{
    settings::Settings,
    shards::{providers::shard_cache::ShardWriterCache, ShardId},
};
use anyhow::bail;
use lazy_static::lazy_static;
use nucliadb_core::tracing::warn;
use nucliadb_core::NodeResult;

use crate::merge::MergePriority;

lazy_static! {
    static ref MERGE_SCHEDULER: OnceLock<MergeScheduler> = OnceLock::new();
}

/// Install merger as the global merge scheduler.
pub fn install_global(merger: MergeScheduler) -> NodeResult<impl FnOnce()> {
    if MERGE_SCHEDULER.get().is_some() {
        bail!("Global merge scheduler has already been installed");
    }
    let global_merger = MERGE_SCHEDULER.get_or_init(move || merger);
    Ok(move || global_merger.run_forever())
}

/// Request a merge to the global merge scheduler.
///
/// This function panics if the global merger hasn't been installed
pub fn request_merge(request: MergeRequest) {
    let merger = MERGE_SCHEDULER.get().expect("Global merge scheduler must be installed");
    merger.schedule(request);
}

/// Merge scheduler is the responsible for scheduling merges in the vectors
/// index. When running, it takes the most prioritary merge request, takes a
/// shard writer and executes the merge.
pub struct MergeScheduler {
    work_queue: Mutex<BinaryHeap<MergeRequest>>,
    shard_cache: Arc<ShardWriterCache>,
    settings: Settings,
}

impl MergeScheduler {
    pub fn new(shard_cache: Arc<ShardWriterCache>, settings: Settings) -> Self {
        Self {
            work_queue: Mutex::new(BinaryHeap::new()),
            shard_cache,
            settings,
        }
    }

    pub fn schedule(&self, request: MergeRequest) {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        queue.push(request);
    }

    fn bulk_schedule(&self, requests: impl Iterator<Item = MergeRequest>) {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        for request in requests {
            queue.push(request);
        }
    }

    pub fn run_forever(&self) {
        loop {
            let result = self.run();
            if let Err(error) = result {
                warn!("An error occurred merging: {}", error);
            }
        }
    }

    /// Take the most prioritary work from the scheduler queue and perform a
    /// merge.
    fn run(&self) -> NodeResult<()> {
        let request = self.next_request();

        // Schedule all shards for a merge when we have no more work to do
        if request.is_none() {
            self.schedule_all_shards();
            return Ok(());
        }
        let request = request.unwrap();

        let shard = self.shard_cache.get(&request.shard_id)?;
        shard.merge()?;

        Ok(())
    }

    fn next_request(&self) -> Option<MergeRequest> {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        let request = queue.pop();
        drop(queue);
        request
    }

    fn schedule_all_shards(&self) {
        let requests = iter_shards(self.settings.shards_path()).map(|shard_id| MergeRequest {
            shard_id,
            priority: MergePriority::WhenFree,
        });
        self.bulk_schedule(requests);
    }
}

#[derive(PartialEq, Eq)]
pub struct MergeRequest {
    pub shard_id: ShardId,
    pub priority: MergePriority,
}

impl PartialOrd for MergeRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

fn iter_shards(shards_path: PathBuf) -> impl Iterator<Item = String> {
    fs::read_dir(shards_path).expect("Can't open shards path").filter_map(|entry| entry.ok()).filter_map(|entry| {
        let path = entry.path();
        if !path.is_dir() {
            return None;
        }

        let shard_folder = path.file_name()?;

        let shard_id = shard_folder.to_str().map(String::from);
        shard_id
    })
}

#[cfg(test)]
mod tests {
    use tempfile::{self, TempDir};

    use super::*;

    fn merge_scheduler() -> (MergeScheduler, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let settings = Settings::builder().data_path(temp_dir.path()).build().unwrap();
        let shard_cache = Arc::new(ShardWriterCache::new(settings.clone()));
        (MergeScheduler::new(shard_cache, settings), temp_dir)
    }

    #[test]
    fn test_merge_scheduler() {
        let (merger, _guards) = merge_scheduler();

        assert_eq!(merger.work_queue.lock().unwrap().len(), 0);

        merger.schedule(MergeRequest {
            shard_id: "shard-id".to_string(),
            priority: MergePriority::default(),
        });
        assert_eq!(merger.work_queue.lock().unwrap().len(), 1);

        // shard is fake, so this fails but work is done
        let result = merger.run();
        assert!(result.is_err());
        assert_eq!(merger.work_queue.lock().unwrap().len(), 0);
    }

    #[test]
    fn test_merger_schedules_all_shards_when_idle() {
        let (merger, _guards) = merge_scheduler();

        // create two fake shards
        let shards_path = merger.settings.shards_path();
        fs::create_dir_all(shards_path.join("shard-a")).unwrap();
        fs::create_dir_all(shards_path.join("shard-b")).unwrap();

        // Run found no work and schedules all shards to merge
        let result = merger.run();
        assert!(result.is_ok());

        assert_eq!(merger.next_request().unwrap().shard_id, "shard-a".to_string());
        assert_eq!(merger.next_request().unwrap().shard_id, "shard-b".to_string());
    }

    #[test]
    fn test_merge_request_priorities() {
        let urgent = MergeRequest {
            shard_id: "urgent".to_string(),
            priority: MergePriority::High,
        };
        let deferrable = MergeRequest {
            shard_id: "deferrable".to_string(),
            priority: MergePriority::Low,
        };
        let not_important = MergeRequest {
            shard_id: "not-important".to_string(),
            priority: MergePriority::WhenFree,
        };

        assert!(urgent > deferrable);
        assert!(urgent > not_important);
        assert!(deferrable < urgent);
        assert!(deferrable > not_important);
        assert!(not_important < urgent);
        assert!(not_important < deferrable);
    }
}
