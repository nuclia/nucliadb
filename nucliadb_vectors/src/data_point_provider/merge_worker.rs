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

use std::path::PathBuf;

use crossbeam::channel::Sender;
use nucliadb_core::tracing::*;
use nucliadb_core::{fs_state, Channel};

use super::merger::{MergeQuery, MergeRequest};
use crate::data_point::{DataPoint, Similarity};
use crate::segment_manager::{SegmentManager, Transaction};
use crate::VectorR;

pub(crate) struct Worker {
    location: PathBuf,
    sender: Sender<Transaction>,
    similarity: Similarity,
    channel: Channel,
}
impl MergeQuery for Worker {
    fn do_work(&self) -> VectorR<()> {
        self.work()
    }
}
impl Worker {
    pub(crate) fn request(
        location: PathBuf,
        sender: Sender<Transaction>,
        similarity: Similarity,
        channel: Channel,
    ) -> MergeRequest {
        Box::new(Worker {
            similarity,
            location,
            sender,
            channel,
        })
    }
    fn work(&self) -> VectorR<()> {
        let subscriber = self.location.as_path();
        let _lock = fs_state::shared_lock(subscriber)?;
        info!("{subscriber:?} is ready to perform a merge");
        let sm = SegmentManager::open(subscriber.to_path_buf())?;
        let work = sm
            .segment_iterator()
            .map(|(id, dlog)| (dlog, *id))
            .collect::<Vec<_>>();

        let new_dp = DataPoint::merge(subscriber, &work, self.similarity, self.channel)?;
        let new_dp_id = new_dp.get_id();

        let mut transaction = Transaction::default();
        transaction.replace_segments(work.iter().map(|(_, id)| *id).collect(), new_dp.get_id());

        if self.sender.send(transaction).is_err() {
            // If the sender has been deallocated this data point becomes garbage,
            // therefore is removed.
            DataPoint::delete(subscriber, new_dp.get_id())?;
        }
        info!("Merge request completed: {new_dp_id}");
        Ok(())
    }
}
