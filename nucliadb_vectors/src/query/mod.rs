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

use crate::data_point::{Address, DataRetriever};

pub struct LabelData {
    value: String,
}
impl LabelData {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        retriever.has_label(x, self.value.as_bytes())
    }
}
pub struct CompountData {
    threshold: usize,
    labels: Vec<LabelData>,
}
impl CompountData {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        let number_of_subqueries = self.labels.len();
        let mut threshold = self.threshold;
        let mut i = 0;
        while threshold > 0 && i <= number_of_subqueries {
            let is_valid = self.labels[i].run(x, retriever);
            threshold -= is_valid as usize;
            i += 1;
        }
        threshold == 0
    }
}

pub enum Query {
    Label(LabelData),
    Compound(CompountData),
}

impl Query {
    pub fn label(label: String) -> Query {
        Query::Label(LabelData { value: label })
    }
    pub fn compound(threshold: usize, labels: Vec<LabelData>) -> Query {
        Query::Compound(CompountData { threshold, labels })
    }
    pub fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Query::Compound(q) => q.run(x, retriever),
            Query::Label(q) => q.run(x, retriever),
        }
    }
}
