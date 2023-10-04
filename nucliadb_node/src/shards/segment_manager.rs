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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::SystemTime;

use crossbeam::channel::{self, Receiver, Sender, TryRecvError};
use nucliadb_core::tracing::debug;

use crate::shards::shard_writer::ShardWriter;

/// If 60 minutes have past since the last modification of a shard, it becomes available for merge.
const SECS_BEFORE_MERGE: u64 = 3600;
/// The minimum number of shards that must be scheduled for merge before sending
/// [`Request::CollectGarbage`]
const GC_THRESHOLD: usize = 5;

pub type RequestSender = Sender<Request>;
pub enum Request {
    WasModified(Arc<ShardWriter>, SystemTime),
    CheckForMerges,
    CollectGarbage,
}

pub struct SegmentManager {
    receiver: Receiver<Request>,
    inner: InnerManager,
}
impl SegmentManager {
    fn run(mut self) {
        loop {
            match self.receiver.try_recv() {
                Err(TryRecvError::Disconnected) => break,
                Err(TryRecvError::Empty) => continue,
                Ok(Request::CheckForMerges) => self.inner.trim_and_merge(),
                Ok(Request::CollectGarbage) => self.inner.collect_garbage(),
                Ok(Request::WasModified(s, t)) => self.inner.record_modification(s, t),
            }
        }
    }
    pub fn install() -> (impl FnOnce(), Sender<Request>) {
        let (sender, receiver) = channel::unbounded();
        let inner = InnerManager {
            my_self: sender.clone(),
            waiting_for_gc: Default::default(),
            record: Default::default(),
        };
        let installer = || SegmentManager { receiver, inner }.run();
        (installer, sender)
    }
}

struct InnerManager {
    my_self: Sender<Request>,
    waiting_for_gc: HashSet<Arc<ShardWriter>>,
    record: HashMap<Arc<ShardWriter>, SystemTime>,
}
impl InnerManager {
    fn record_modification(&mut self, shard: Arc<ShardWriter>, update_time: SystemTime) {
        self.record.insert(shard, update_time);
    }
    fn trim_and_merge(&mut self) {
        let active_shards = std::mem::take(&mut self.record);
        for (active_shard, last_update) in active_shards {
            let Ok(secs_elapsed) = last_update.elapsed().map(|t| t.as_secs()) else {
                continue;
            };
            if secs_elapsed <= SECS_BEFORE_MERGE {
                self.record.insert(active_shard, last_update);
                continue;
            }
            match active_shard.merge() {
                Err(err) => {
                    // TODO: we can try retry policies
                    debug!("Merge error in {} :: {err:?}", active_shard.id)
                }
                Ok(_) => {
                    debug!("Successful merge in {}", active_shard.id);
                    self.waiting_for_gc.insert(active_shard);
                }
            }
        }
        if self.waiting_for_gc.len() >= GC_THRESHOLD {
            // Is safe to ignore the error because:
            // -> TrySendError::Disconnected errors are not possible since the receiver and self
            // need to be dropped at the same time.
            // -> TrySendError::Full errors are not possible since the channel is unbounded.
            let _ = self.my_self.try_send(Request::CollectGarbage);
        }
    }
    fn collect_garbage(&mut self) {
        let eligible_for_gc = std::mem::take(&mut self.waiting_for_gc);
        for eligible_shard in eligible_for_gc {
            match eligible_shard.gc() {
                Ok(_) => debug!("GC successful in {}", eligible_shard.id),
                Err(err) => debug!("GC error in {} :: {err:?}", eligible_shard.id),
            }
        }
    }
}
