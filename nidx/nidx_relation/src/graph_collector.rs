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

use std::collections::HashSet;

use tantivy::{
    DocId, Score, SegmentOrdinal, SegmentReader,
    collector::{Collector, SegmentCollector},
    columnar::Column,
};

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
    limit: usize,
    unique: HashSet<Vec<u64>>,
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
    type Fruit = HashSet<Vec<u64>>;
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
            limit: self.limit,
            unique: HashSet::new(),
            encoded_node_reader: fast_field_reader,
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut unique = HashSet::new();
        let mut fruits = segment_fruits.into_iter().flatten();
        let mut fruit = fruits.next();

        while fruit.is_some() && unique.len() < self.limit {
            unique.insert(fruit.unwrap());
            fruit = fruits.next();
        }
        Ok(unique)
    }
}

impl SegmentCollector for TopUniqueNodeSegmentCollector2 {
    type Fruit = HashSet<Vec<u64>>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        // we already have all unique results we need
        if self.unique.len() >= self.limit {
            return;
        }
        let encoded_node = self.encoded_node_reader.values_for_doc(doc_id).collect::<Vec<u64>>();
        self.unique.insert(encoded_node);
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
        false
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

/// Top unique N
///
/// Maintain the top set of unique keys with greatest scores.
pub struct TopUniqueN<K> {
    top_n: usize,
    elements: HashMap<K, f32>,
}

impl<K> TopUniqueN<K>
where
    K: Eq + std::hash::Hash + std::fmt::Debug,
{
    pub fn new(top_n: usize) -> Self {
        Self {
            top_n,
            elements: HashMap::new(),
        }
    }

    pub fn insert(&mut self, key: K, score: f32) {
        println!("[before] insert {key:?} ({score}) into {:?}", self.elements);
        match self.elements.entry(key) {
            std::collections::hash_map::Entry::Vacant(vacant) => {
                vacant.insert(score);
            }
            std::collections::hash_map::Entry::Occupied(mut occupied) => {
                if score > *occupied.get() {
                    occupied.insert(score);
                }
            }
        };
        println!("[after] inserted: {:?}", self.elements);
        println!();
    }

    pub fn into_sorted_vec(self) -> Vec<(K, f32)> {
        let mut vec = Vec::from_iter(self.elements.into_iter());
        vec.sort_by(|a, b| a.1.total_cmp(&b.1).reverse());
        vec.truncate(self.top_n);
        vec
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_unique_n() {
        let mut top = TopUniqueN::new(3);
        top.insert("A", 1.0); // A=1.0 (insert A)
        top.insert("B", 2.0); // A=1.0, B=2.0 (insert B)
        top.insert("B", 3.0); // A=1.0, B=3.0 (replace B)
        top.insert("C", 3.0); // A=1.0, B=3.0, C=3.0 (insert C)
        top.insert("D", 4.0); // B=3.0, C=3.0, D=4.0 (insert D, remove A)
        top.insert("E", 1.5); // B=3.0, C=3.0, D=4.0 (do not insert E)

        let r = top.into_sorted_vec();
        println!("Result vec: {r:?}");
        assert_eq!(r.len(), 3);
        assert_eq!(r[0], ("D", 4.0));
        assert!((r[1] == ("B", 3.0) && r[2] == ("C", 3.0)) || (r[1] == ("C", 3.0) && r[2] == ("B", 3.0)));
    }
}
