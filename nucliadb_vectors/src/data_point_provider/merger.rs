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
use nucliadb_core::tracing;
use std::collections::VecDeque;
use std::sync::{Condvar, Mutex};

use crate::VectorR;

pub type MergeRequest = Box<dyn MergeQuery>;

pub trait MergeQuery: Send {
    fn do_work(&self) -> VectorR<()>;
}

lazy_static::lazy_static! {
    static ref MERGER_SCHEDULER: Merger = Merger::default();
}

pub fn send_merge_request(key: String, request: MergeRequest) {
    MERGER_SCHEDULER.enqueue(key, request);
}

#[derive(Default)]
pub struct Merger {
    queue: Mutex<VecDeque<(String, MergeRequest)>>,
    condvar: Condvar,
}

impl Merger {
    fn enqueue(&self, key: String, request: MergeRequest) {
        let mut queue = self.queue.lock().expect("Poisoned Merger scheduler mutex");
        if queue.iter().any(|e| e.0 == key) {
            tracing::info!("Skipping dedup merge request");
            return;
        }
        queue.push_back((key, request));
        self.condvar.notify_all();
    }

    pub fn install_global() -> VectorR<impl FnOnce()> {
        Ok(move || MERGER_SCHEDULER.run())
    }
    fn run(&self) {
        loop {
            let mut queue = self.queue.lock().expect("Poisoned Merger scheduler mutex");
            while queue.is_empty() {
                queue = self.condvar.wait(queue).expect("Poisoned Merger scheduler mutex");
            }
            let (_, task) = queue.pop_front().unwrap();
            drop(queue);
            match task.do_work() {
                Ok(()) => (),
                Err(err) => tracing::info!("error merging: {err}"),
            }
        }
    }
}
