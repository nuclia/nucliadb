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
use std::sync::Condvar;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use anyhow::anyhow;
use nucliadb_core::vectors::MergeMetrics;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};

use crate::shards::ShardId;
use crate::{settings::Settings, shards::providers::shard_cache::ShardWriterCache};
use nucliadb_core::tracing::warn;
use nucliadb_core::NodeResult;

use super::work::WorkQueue;
use crate::merge::{MergePriority, MergeRequest, MergeWaiter};

// Time between scheduler being idle and scheduling all shards for merge
#[cfg(not(test))]
const SCHEDULE_ALL_SHARDS_DELAY: Duration = Duration::from_secs(10);
#[cfg(test)]
const SCHEDULE_ALL_SHARDS_DELAY: Duration = Duration::from_millis(50);

/// Merge scheduler is the responsible for scheduling merges in the vectors
/// index. When running, it takes the most prioritary merge request, takes a
/// shard writer and executes the merge.
pub struct MergeScheduler {
    work_queue: Mutex<WorkQueue<InternalMergeRequest>>,
    shard_cache: Arc<ShardWriterCache>,
    settings: Settings,
    condvar: Condvar,
    shutdown: Mutex<bool>,
}

impl MergeScheduler {
    pub fn new(shard_cache: Arc<ShardWriterCache>, settings: Settings) -> Self {
        Self {
            work_queue: Mutex::new(WorkQueue::new()),
            shard_cache,
            settings,
            condvar: Condvar::default(),
            shutdown: Mutex::new(false),
        }
    }

    /// Schedule a new merge request. The work handle returned can be used to
    /// wait for the merge result.
    pub fn schedule(&self, request: MergeRequest) -> WorkHandle {
        let (request, priority, handle) = self.prepare(request);

        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        queue.push(request, priority);
        drop(queue);

        // Awake scheduler if it's asleep
        self.condvar.notify_all();

        handle
    }

    /// Runs the scheduler until stopped
    pub fn run(&self) {
        let mut shutdown = self.shutdown.lock().expect("Poisoned merger shutdown signal");
        *shutdown = false;
        drop(shutdown);

        loop {
            let merge = self.process();
            if let Err(error) = merge {
                warn!("An error occurred while merging: {}", error);
            }
        }
    }

    /// Signals the scheduler to stop
    pub fn stop(&self) {
        let mut shutdown = self.shutdown.lock().expect("Poisoned merger shutdown signal");
        *shutdown = true;
    }

    /// Take the most prioritary work from the scheduler queue and perform a
    /// merge
    fn process(&self) -> NodeResult<()> {
        let request = self.blocking_next();
        let result = self.merge_shard(&request.shard_id);

        // When a notifier is requested, send the merge result and let the
        // caller be responsible to handle errors
        match request.notifier {
            Some(notifier) => {
                let _ = notifier.send(result);
            }
            None => {
                result?;
            }
        }

        Ok(())
    }

    // Get the next merge request. If there's no more available, wait until
    // someone requests or, after a timeout, schedule all shards for merge and
    // get 1 request
    fn blocking_next(&self) -> InternalMergeRequest {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        while queue.is_empty() {
            let wait;
            (queue, wait) =
                self.condvar.wait_timeout(queue, SCHEDULE_ALL_SHARDS_DELAY).expect("Poisoned merger scheduler mutex");

            if wait.timed_out() {
                // Nobody requested a merge and it's been a while since last,
                // let's schedule all shards for a merge
                for (request, priority) in self.all_shards_requests() {
                    queue.push(request, priority);
                }
            }
        }
        queue.pop().unwrap()
    }

    fn prepare(&self, request: MergeRequest) -> (InternalMergeRequest, MergePriority, WorkHandle) {
        let mut internal_request = InternalMergeRequest {
            shard_id: request.shard_id,
            notifier: None,
        };
        let mut handle = WorkHandle {
            waiter: None,
        };

        match request.waiter {
            MergeWaiter::None => {}
            MergeWaiter::Async => {
                let (tx, rx) = oneshot::channel();
                internal_request.notifier = Some(tx);
                handle.waiter = Some(rx);
            }
        };

        (internal_request, request.priority, handle)
    }

    fn all_shards_requests(&self) -> impl Iterator<Item = (InternalMergeRequest, MergePriority)> + '_ {
        iter_shards(self.settings.shards_path())
            .map(|shard_id| MergeRequest {
                shard_id,
                priority: MergePriority::WhenFree,
                waiter: MergeWaiter::None,
            })
            .map(|request| {
                let (request, priority, _handle) = self.prepare(request);
                (request, priority)
            })
    }

    fn merge_shard(&self, shard_id: &ShardId) -> NodeResult<MergeMetrics> {
        let shard = self.shard_cache.get(shard_id)?;
        shard.merge()
    }

    #[cfg(test)]
    pub fn pending_work(&self) -> usize {
        self.work_queue.lock().unwrap().len()
    }
}

struct InternalMergeRequest {
    shard_id: String,
    notifier: Option<Sender<NodeResult<MergeMetrics>>>,
}

impl PartialEq for InternalMergeRequest {
    fn eq(&self, other: &Self) -> bool {
        self.shard_id.eq(&other.shard_id)
    }
}

pub struct WorkHandle {
    waiter: Option<Receiver<NodeResult<MergeMetrics>>>,
}

impl WorkHandle {
    pub async fn wait(self) -> NodeResult<MergeMetrics> {
        match self.waiter {
            Some(waiter) => waiter.await?,
            None => Err(anyhow!("There was no waiter for this handle")),
        }
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
    use tokio;

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

        assert_eq!(merger.pending_work(), 0);

        merger.schedule(MergeRequest {
            shard_id: "shard-id".to_string(),
            priority: MergePriority::default(),
            waiter: MergeWaiter::None,
        });
        assert_eq!(merger.pending_work(), 1);

        // shard is fake, so this fails but work is done
        let result = merger.process();
        assert!(result.is_err());
        assert_eq!(merger.pending_work(), 0);
    }

    #[tokio::test]
    async fn test_merger_with_waiter() {
        let (merger, _guards) = merge_scheduler();

        let handle = merger.schedule(MergeRequest {
            shard_id: "shard-id".to_string(),
            priority: MergePriority::default(),
            waiter: MergeWaiter::None,
        });

        let merge = handle.wait().await;
        // merge is an error because shard doesn't exist
        assert!(merge.is_err())
    }

    #[test]
    fn test_merger_schedules_all_shards_when_idle() {
        let (merger, _guards) = merge_scheduler();

        // create two fake shards
        let shards_path = merger.settings.shards_path();
        fs::create_dir_all(shards_path.join("shard-a")).unwrap();
        fs::create_dir_all(shards_path.join("shard-b")).unwrap();

        // this call will block, wait and schedule all shards
        merger.blocking_next();
        assert_eq!(merger.pending_work(), 1);

        // now it'll get without blocking
        merger.blocking_next();
        assert_eq!(merger.pending_work(), 0);
    }
}
