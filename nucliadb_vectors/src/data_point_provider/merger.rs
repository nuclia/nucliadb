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

use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Once;

use nucliadb_core::tracing;

use crate::{VectorErr, VectorR};

pub type MergeRequest = Box<dyn MergeQuery>;
pub type MergeTxn = Sender<MergeRequest>;

pub trait MergeQuery: Send {
    fn do_work(&self) -> VectorR<()>;
}

#[derive(Clone)]
struct MergerHandle(MergeTxn);
impl MergerHandle {
    pub fn send(&self, request: MergeRequest) {
        let Err(e) = self.0.send(request) else { return };
        tracing::info!("Error sending merge request, {e}");
    }
}

static mut MERGER_NOTIFIER: Option<MergerHandle> = None;
static MERGER_NOTIFIER_SET: Once = Once::new();

pub fn send_merge_request(request: MergeRequest) {
    // It is always safe to read from MERGER_NOTIFIER since
    // it can only be writen through MERGER_NOTIFIER_SET and is not exposed in the public interface.
    // MERGER_NOTIFIER_SET is protected by the type Once so we avoid concurrency problems.
    match unsafe { &MERGER_NOTIFIER } {
        Some(merger) => merger.send(request),
        None => tracing::warn!("Merge requests are being sent without a merger intalled"),
    }
}

pub struct Merger {
    rtxn: Receiver<MergeRequest>,
}

impl Merger {
    pub fn install_global() -> VectorR<impl FnOnce()> {
        let mut status = Err(VectorErr::MergerAlreadyInitialized);
        MERGER_NOTIFIER_SET.call_once(|| unsafe {
            let (stxn, rtxn) = mpsc::channel();
            let handler = MergerHandle(stxn);
            // It is safe to initialize MERGER_NOTIFIER
            // since the setter can only be called once.
            MERGER_NOTIFIER = Some(handler);
            status = Ok(|| Merger { rtxn }.run());
        });
        status
    }
    fn run(self) {
        loop {
            match self.rtxn.recv() {
                Err(err) => tracing::info!("channel error {}", err),
                Ok(query) => match query.do_work() {
                    Ok(()) => (),
                    Err(err) => tracing::info!("error merging: {err}"),
                },
            }
        }
    }
}
