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

use std::sync::Arc;

use super::merger::{self, MergeQuery, MergeRequest};
use super::IndexInner;
use crate::VectorR;

pub(crate) struct Worker {
    index: Arc<IndexInner>,
}
impl MergeQuery for Worker {
    fn do_work(&self) -> VectorR<()> {
        self.work()
    }
}
impl Worker {
    pub(crate) fn request(index: Arc<IndexInner>) -> MergeRequest {
        Box::new(Worker {
            index,
        })
    }
    fn work(&self) -> VectorR<()> {
        let more = self.index.do_merge();

        if more {
            merger::send_merge_request(
                self.index.location.to_string_lossy().into(),
                Worker::request(self.index.clone()),
            )
        }
        Ok(())
    }
}
