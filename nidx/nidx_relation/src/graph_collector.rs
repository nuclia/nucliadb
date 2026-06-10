// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

use tantivy::{
    DocId, Score, SegmentOrdinal, SegmentReader,
    collector::{Collector, SegmentCollector},
    columnar::Column,
};

use crate::top_unique_n::TopUniqueN;

#[derive(Clone, Copy)]
pub enum Selector {
    SourceNodes,
    DestinationNodes,
    Relations,
}

pub struct TopUniqueCollector {
    limit: usize,
    selector: Selector,
}

pub struct TopUniqueSegmentCollector {
    unique: TopUniqueN<Vec<u64>>,
    encoded_field: Column<u64>,
}

impl TopUniqueCollector {
    pub fn new(selector: Selector, top_k: usize) -> Self {
        Self { limit: top_k, selector }
    }
}

impl Collector for TopUniqueCollector {
    type Fruit = TopUniqueN<Vec<u64>>;
    type Child = TopUniqueSegmentCollector;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        let fast_field = match self.selector {
            Selector::SourceNodes => segment.fast_fields().u64("encoded_source_id")?,
            Selector::DestinationNodes => segment.fast_fields().u64("encoded_target_id")?,
            Selector::Relations => segment.fast_fields().u64("encoded_relation_id")?,
        };
        let segment_collector = TopUniqueSegmentCollector {
            unique: TopUniqueN::new(self.limit),
            encoded_field: fast_field,
        };
        Ok(segment_collector)
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let fruits = segment_fruits.into_iter().flat_map(|top| top.into_sorted_vec());
        let mut unique = TopUniqueN::new(self.limit);
        for (key, score) in fruits {
            unique.insert(key, score);
        }
        Ok(unique)
    }
}

impl SegmentCollector for TopUniqueSegmentCollector {
    type Fruit = TopUniqueN<Vec<u64>>;

    fn collect(&mut self, doc_id: DocId, score: Score) {
        let value = self.encoded_field.values_for_doc(doc_id).collect::<Vec<u64>>();
        self.unique.insert(value, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.unique
    }
}
