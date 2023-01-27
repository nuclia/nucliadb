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

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use nucliadb_core::thread;

pub type MergeRequest = Box<dyn MergeQuery>;
pub type MergeTxn = Sender<MergeRequest>;

pub trait MergeQuery: Send {
    fn do_work(&self) -> Result<(), String>;
}

struct Updater {
    rtxn: Receiver<MergeRequest>,
}

impl Updater {
    fn new(rtxn: Receiver<MergeRequest>) -> Updater {
        Updater { rtxn }
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

lazy_static::lazy_static! {
    static ref MERGER: Mutex<MergeTxn> = {
        let (stxn, rtxn) = channel();
        let updater = Updater::new(rtxn);
        thread::spawn(move || updater.run());
        Mutex::new(stxn)
    };
}

pub fn get_notifier() -> MergeTxn {
    MERGER.lock().unwrap().clone()
}
