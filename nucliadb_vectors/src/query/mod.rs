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

#[derive(Debug, Clone)]
pub struct LabelData {
    value: String,
}
impl LabelData {
    fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        retriever.has_label(x, self.value.as_bytes())
    }
}

#[derive(Debug, Clone)]
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

#[derive(Debug, Clone)]
pub enum Query {
    Label(LabelData),
    Compound(CompountData),
}

impl Query {
    pub fn label(label: String) -> Query {
        Query::Label(LabelData { value: label })
    }
    pub fn compound(threshold: usize, labels: Vec<String>) -> Query {
        let labels = labels
            .into_iter()
            .map(|value| LabelData { value })
            .collect();
        Query::Compound(CompountData { threshold, labels })
    }
    pub fn run<D: DataRetriever>(&self, x: Address, retriever: &D) -> bool {
        match self {
            Query::Compound(q) => q.run(x, retriever),
            Query::Label(q) => q.run(x, retriever),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;
    use crate::data_point::Address;
    struct DummyRetriever {
        labels: HashSet<&'static [u8]>,
    }
    impl DataRetriever for DummyRetriever {
        fn has_label(&self, _: Address, label: &[u8]) -> bool {
            self.labels.contains(label)
        }
        fn is_deleted(&self, _: Address) -> bool {
            panic!("Not mean to be used")
        }
        fn consine_similarity(&self, _: Address, _: Address) -> f32 {
            panic!("Not mean to be used")
        }
        fn get_vector(&self, _: Address) -> &[u8] {
            panic!("Not mean to be used")
        }
    }
    #[test]
    fn test_query() {
        const L1: &[u8] = b"Label1";
        const L2: &[u8] = b"Label2";
        const L3: &[u8] = b"Label3";
        const ADDRESS: Address = Address::dummy();
        let retriever = DummyRetriever {
            labels: [L1, L3].into_iter().collect(),
        };
        let queries = [
            Query::label(String::from_utf8_lossy(L1).to_string()),
            Query::label(String::from_utf8_lossy(L3).to_string()),
        ];
        assert!(queries.iter().all(|q| q.run(ADDRESS, &retriever)));
        let queries = [
            Query::label(String::from_utf8_lossy(L1).to_string()),
            Query::label(String::from_utf8_lossy(L2).to_string()),
        ];
        assert!(!queries.iter().all(|q| q.run(ADDRESS, &retriever)));
        let labels = vec![
            String::from_utf8_lossy(L1).to_string(),
            String::from_utf8_lossy(L2).to_string(),
        ];
        assert!(Query::compound(1, labels).run(ADDRESS, &retriever));
    }
}
