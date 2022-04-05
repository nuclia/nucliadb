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

use crate::graph_disk::LockDisk;
use crate::graph_elems::*;
use crate::query::Query;
#[derive(Clone)]
pub struct FindLabelsValue {
    pub found: Vec<LabelId>,
    pub not_found: Vec<String>,
    pub min_reached: usize,
}
impl Default for FindLabelsValue {
    fn default() -> Self {
        Self {
            found: Vec::default(),
            not_found: Vec::default(),
            min_reached: usize::MAX,
        }
    }
}

pub struct FindLabelsQuery<'a> {
    pub labels: Vec<String>,
    pub disk: &'a LockDisk,
}

impl<'a> Query for FindLabelsQuery<'a> {
    type Output = FindLabelsValue;

    fn run(&mut self) -> Self::Output {
        let mut result = FindLabelsValue::default();
        for label in std::mem::take(&mut self.labels) {
            match self.disk.get_label_id(&label) {
                Some(l) => {
                    let label_data = self.disk.get_label(l);
                    result.min_reached = std::cmp::min(label_data.reached_by, result.min_reached);
                    result.found.push(l);
                }
                None => {
                    result.min_reached = 0;
                    result.not_found.push(label);
                }
            }
        }
        result
    }
}
