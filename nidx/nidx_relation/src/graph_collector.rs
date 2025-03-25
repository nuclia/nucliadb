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
pub enum NodeSelector {
    SourceNodes,
    DestinationNodes,
}

// Node collector for schema v2
//
// We can now use fast fields to uniquely identify nodes.

pub struct TopUniqueNodeCollector2 {
    limit: usize,
    selector: NodeSelector,
}

pub struct TopUniqueNodeSegmentCollector2 {
    unique: TopUniqueN<Vec<u64>>,
    encoded_node_reader: Column<u64>,
}

// Relations collector for schema v2
pub struct TopUniqueRelationCollector2 {
    limit: usize,
}

pub struct TopUniqueRelationSegmentCollector2 {
    unique: TopUniqueN<Vec<u64>>,
    encoded_relation_reader: Column<u64>,
}

impl TopUniqueNodeCollector2 {
    pub fn new(selector: NodeSelector, limit: usize) -> Self {
        Self { limit, selector }
    }
}

impl Collector for TopUniqueNodeCollector2 {
    type Fruit = TopUniqueN<Vec<u64>>;
    type Child = TopUniqueNodeSegmentCollector2;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        let fast_field_reader = match self.selector {
            NodeSelector::SourceNodes => segment.fast_fields().u64("encoded_source_id")?,
            NodeSelector::DestinationNodes => segment.fast_fields().u64("encoded_target_id")?,
        };
        Ok(TopUniqueNodeSegmentCollector2 {
            unique: TopUniqueN::new(self.limit),
            encoded_node_reader: fast_field_reader,
        })
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

impl SegmentCollector for TopUniqueNodeSegmentCollector2 {
    type Fruit = TopUniqueN<Vec<u64>>;

    fn collect(&mut self, doc_id: DocId, score: Score) {
        let encoded_node = self.encoded_node_reader.values_for_doc(doc_id).collect::<Vec<u64>>();
        self.unique.insert(encoded_node, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.unique
    }
}

impl TopUniqueRelationCollector2 {
    pub fn new(limit: usize) -> Self {
        Self { limit }
    }
}

impl Collector for TopUniqueRelationCollector2 {
    type Fruit = Vec<Vec<u64>>;
    type Child = TopUniqueRelationSegmentCollector2;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(TopUniqueRelationSegmentCollector2 {
            unique: TopUniqueN::new(self.limit),
            encoded_relation_reader: segment.fast_fields().u64("encoded_relation_id")?,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let fruits = segment_fruits.into_iter().flat_map(|map| map.into_sorted_vec());

        let mut unique = TopUniqueN::new(self.limit);
        for (key, score) in fruits {
            unique.insert(key, score);
        }

        Ok(unique.into_sorted_vec().into_iter().map(|(key, _score)| key).collect())
    }
}

impl SegmentCollector for TopUniqueRelationSegmentCollector2 {
    type Fruit = TopUniqueN<Vec<u64>>;

    fn collect(&mut self, doc_id: DocId, score: Score) {
        let relation = self
            .encoded_relation_reader
            .values_for_doc(doc_id)
            .collect::<Vec<u64>>();
        self.unique.insert(relation, score);
    }

    fn harvest(self) -> Self::Fruit {
        self.unique
    }
}
