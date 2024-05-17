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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Condvar;
use std::time::Duration;
use std::{
    path::PathBuf,
    sync::{Arc, Mutex},
};

use crate::cache::ShardWriterCache;
use crate::settings::Settings;
use anyhow::anyhow;
use nucliadb_core::merge::MergeRequester;
use nucliadb_core::metrics::vectors::MergeSource;
use nucliadb_core::tracing::warn;
use nucliadb_core::vectors::MergeMetrics;
use nucliadb_core::vectors::{MergeContext, MergeParameters};
use nucliadb_core::NodeResult;
use tokio::sync::oneshot;
use tokio::sync::oneshot::{Receiver, Sender};

use super::work::WorkQueue;
use crate::merge::{MergePriority, MergeRequest, MergeWaiter};

/// Merge scheduler is the responsible for scheduling merges in the vectors
/// index. When running, it takes the most prioritary merge request, takes a
/// shard writer and executes the merge.
pub struct MergeScheduler {
    work_queue: Mutex<WorkQueue<InternalMergeRequest>>,
    shard_cache: Arc<ShardWriterCache>,
    settings: Settings,
    condvar: Condvar,
    shutdown: AtomicBool,
    on_commit_parameters: MergeParameters,
    scheduler_parameters: MergeParameters,
}

impl MergeScheduler {
    pub fn new(shard_cache: Arc<ShardWriterCache>, settings: Settings) -> Self {
        let idle_parameters = MergeParameters {
            max_nodes_in_merge: settings.merge_scheduler_max_nodes_in_merge,
            segments_before_merge: settings.merge_scheduler_segments_before_merge,
            maximum_deleted_entries: settings.merge_maximum_deleted_entries,
        };

        let on_commit_parameters = MergeParameters {
            max_nodes_in_merge: settings.merge_on_commit_max_nodes_in_merge,
            segments_before_merge: settings.merge_on_commit_segments_before_merge,
            maximum_deleted_entries: settings.merge_maximum_deleted_entries,
        };

        Self {
            shard_cache,
            settings,
            work_queue: Mutex::new(WorkQueue::new()),
            condvar: Condvar::default(),
            shutdown: AtomicBool::new(false),
            on_commit_parameters,
            scheduler_parameters: idle_parameters,
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
    pub fn run_forever(&self) {
        self.shutdown.store(false, Ordering::Relaxed);

        loop {
            self.run();

            if self.shutdown.load(Ordering::Relaxed) {
                break;
            }
        }
    }

    /// Signals the scheduler to stop
    pub fn stop(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    fn run(&self) {
        let work = self.wait_for_work(self.settings.merge_scheduler_free_time_work_scheduling_delay);

        match work {
            Some(request) => {
                let merge = self.process(request);
                if let Err(error) = merge {
                    warn!("An error occurred while merging: {}", error);
                }
            }
            None => {
                // Nobody requested a merge and it's been a while since last
                // merge, let's schedule some work to do in our free time
                self.schedule_free_time_work();
            }
        }
    }

    fn wait_for_work(&self, timeout: Duration) -> Option<InternalMergeRequest> {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");

        // Wait in a loop because a condvar may get awaken spuriously without new work being inserted
        loop {
            if let Some(work) = queue.pop() {
                return Some(work);
            }

            let wait;
            (queue, wait) = self.condvar.wait_timeout(queue, timeout).expect("Poisoned merger scheduler mutex");
            if wait.timed_out() {
                return None;
            }
        }
    }

    fn prepare(&self, request: MergeRequest) -> (InternalMergeRequest, MergePriority, WorkHandle) {
        let metrics_source = match request.priority {
            MergePriority::WhenFree => MergeSource::Idle,
            MergePriority::Low => MergeSource::Low,
            MergePriority::High => MergeSource::High,
        };

        let mut internal_request = InternalMergeRequest {
            shard_id: request.shard_id,
            notifier: None,
            metrics_source,
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

    fn process(&self, request: InternalMergeRequest) -> NodeResult<()> {
        let Some(shard) = self.shard_cache.peek(&request.shard_id) else {
            // Only shards that are already cached will be
            // processed.
            return Ok(());
        };
        let merge_context = MergeContext {
            source: request.metrics_source,
            parameters: if request.metrics_source == MergeSource::Low {
                self.on_commit_parameters
            } else {
                self.scheduler_parameters
            },
        };
        let result = shard.merge(merge_context);

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

    fn schedule_free_time_work(&self) {
        let mut queue = self.work_queue.lock().expect("Poisoned merger scheduler mutex");
        for (request, priority) in self.all_shards_requests() {
            queue.push(request, priority);
        }
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

    #[cfg(test)]
    pub fn pending_work(&self) -> usize {
        self.work_queue.lock().unwrap().len()
    }
}

impl MergeRequester for MergeScheduler {
    fn request_merge(&self, request: MergeRequest) {
        self.schedule(request);
    }
}

struct InternalMergeRequest {
    shard_id: String,
    notifier: Option<Sender<NodeResult<MergeMetrics>>>,
    metrics_source: MergeSource,
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
    use std::fs;
    use std::thread;
    use tempfile::{self, TempDir};
    use tokio;

    use crate::disk_structure;
    use crate::settings::EnvSettings;

    use super::*;

    fn merge_scheduler() -> (MergeScheduler, TempDir) {
        let temp_dir = tempfile::tempdir().unwrap();
        let settings: Settings = EnvSettings {
            data_path: temp_dir.path().to_owned(),
            merge_scheduler_free_time_work_scheduling_delay: Duration::from_millis(5),
            ..Default::default()
        }
        .into();
        let shard_cache = Arc::new(ShardWriterCache::new(settings.clone()));
        fs::create_dir_all(settings.shards_path()).unwrap();

        (MergeScheduler::new(shard_cache, settings), temp_dir)
    }

    #[test]
    fn test_schedule_merge_requests() {
        let (merger, _guards) = merge_scheduler();
        assert_eq!(merger.pending_work(), 0);

        for i in 0..5 {
            merger.schedule(MergeRequest {
                shard_id: format!("shard-{i}"),
                priority: MergePriority::default(),
                waiter: MergeWaiter::None,
            });
        }
        assert_eq!(merger.pending_work(), 5);
    }

    #[test]
    fn test_request_dedup() {
        let (merger, _guards) = merge_scheduler();
        assert_eq!(merger.pending_work(), 0);

        for _ in 0..2 {
            merger.schedule(MergeRequest {
                shard_id: "shard-id".to_string(),
                priority: MergePriority::default(),
                waiter: MergeWaiter::None,
            });
        }
        // repeated requests are deduplicated
        assert_eq!(merger.pending_work(), 1);
    }

    #[test]
    fn test_request_processing() {
        let (merger, _guards) = merge_scheduler();

        const TOTAL: usize = 3;
        for i in 0..TOTAL {
            merger.schedule(MergeRequest {
                shard_id: format!("shard-{i}"),
                priority: MergePriority::default(),
                waiter: MergeWaiter::None,
            });
        }

        assert_eq!(merger.pending_work(), TOTAL);
        for i in 0..TOTAL {
            // shard is fake, so it will fail but work will be done
            merger.run();
            assert_eq!(merger.pending_work(), TOTAL - i - 1);
        }
        assert_eq!(merger.pending_work(), 0);
    }

    #[tokio::test]
    async fn test_merge_and_wait_results() {
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
    fn test_free_time_work_scheduling() {
        let (merger, _guards) = merge_scheduler();

        // create two fake shards
        let shards_path = merger.settings.shards_path();
        fs::create_dir_all(disk_structure::shard_path_by_id(&shards_path, "shard-a")).unwrap();
        fs::create_dir_all(disk_structure::shard_path_by_id(&shards_path, "shard-b")).unwrap();

        assert_eq!(merger.pending_work(), 0);
        merger.schedule_free_time_work();
        assert_eq!(merger.pending_work(), 2);
    }

    #[test]
    fn test_scheduler_shutdown() {
        let (merger, _guards) = merge_scheduler();
        let merger = Arc::new(merger);
        let merger_run = Arc::clone(&merger);
        let merger_stop = Arc::clone(&merger);

        let t_run = thread::spawn(move || {
            merger_run.run_forever();
        });
        let t_stop = thread::spawn(move || {
            thread::sleep(Duration::from_millis(1));
            merger_stop.stop();
        });

        // We wait for t_run first so we can't end the test without executing
        // it. After 1 ms, t_stop will stop the merger and finish, unblocking
        // t_run and this test will finish too
        t_run.join().unwrap();
        t_stop.join().unwrap();
    }
}
