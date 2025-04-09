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
