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

use lazy_static;
use nucliadb_core::tracing::debug;
use std::{
    collections::VecDeque,
    sync::{Arc, Condvar, Mutex},
};

use crate::VectorR;

use super::IndexInner;

lazy_static::lazy_static! {
    static ref MERGE_SCHEDULER: Merger = Merger::default();
}

pub fn install_global() -> VectorR<impl FnOnce()> {
    Ok(move || MERGE_SCHEDULER.run_forever())
}

pub fn send_merge_request(key: String, request: MergeRequest) {
    MERGE_SCHEDULER.request_merge(key, request);
}

#[derive(Default)]
pub struct Merger {
    queue: Mutex<VecDeque<(String, MergeRequest)>>,
    condvar: Condvar,
}

impl Merger {
    fn run_forever(&self) {
        loop {
            let mut queue = self.queue.lock().expect("Poisoned Merger scheduler mutex");
            while queue.is_empty() {
                queue = self.condvar.wait(queue).expect("Poisoned Merger scheduler mutex");
            }
            let (_, request) = queue.pop_front().unwrap();
            drop(queue);
            request.run();
        }
    }

    fn request_merge(&self, key: String, request: MergeRequest) {
        let mut queue = self.queue.lock().expect("Poisoned Merger scheduler mutex");
        if queue.iter().any(|e| e.0 == key) {
            debug!("Skipping dedup merge request");
            return;
        }
        queue.push_back((key, request));
        self.condvar.notify_all();
    }
}

pub struct MergeRequest {
    index: Arc<IndexInner>,
}

impl MergeRequest {
    pub fn new(index: Arc<IndexInner>) -> Self {
        Self {
            index,
        }
    }

    fn run(self) {
        let more = self.index.do_merge();

        if more {
            // if we can merge more, schedule another merge for the same index
            send_merge_request(self.index.location.to_string_lossy().into(), self)
        }
    }
}
