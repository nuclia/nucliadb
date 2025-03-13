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

use std::collections::{HashMap, HashSet};

use tantivy::{
    DocId, Score, SegmentOrdinal, SegmentReader, TantivyDocument,
    collector::{Collector, SegmentCollector},
    store::StoreReader,
};
use tracing::warn;

use crate::io_maps;

// XXX: this is a completely arbitrary number. Feel free to change for a more adequate value
const STORE_READER_CACHED_BLOCKS: usize = 10;

pub type NodeId = (String, i32, String);
pub type RelationId = (String, u64);

#[derive(Clone, Copy)]
pub enum NodeSelector {
    SourceNodes,
    DestinationNodes,
}

pub struct TopUniqueNodeCollector {
    limit: usize,
    selector: NodeSelector,
    schema: crate::schema::Schema,
}

pub struct TopUniqueRelationCollector {
    limit: usize,
    schema: crate::schema::Schema,
}

pub struct TopUniqueNodeSegmentCollector {
    limit: usize,
    selector: NodeSelector,
    unique: HashSet<NodeId>,
    schema: crate::schema::Schema,
    store_reader: StoreReader,
}

pub struct TopUniqueRelationSegmentCollector {
    limit: usize,
    unique: HashMap<RelationId, TantivyDocument>,
    schema: crate::schema::Schema,
    store_reader: StoreReader,
}

impl TopUniqueNodeCollector {
    pub fn new(selector: NodeSelector, limit: usize) -> Self {
        Self {
            limit,
            selector,
            schema: crate::schema::Schema::new(),
        }
    }
}

impl TopUniqueRelationCollector {
    pub fn new(limit: usize) -> Self {
        Self {
            limit,
            schema: crate::schema::Schema::new(),
        }
    }
}

impl Collector for TopUniqueNodeCollector {
    type Fruit = HashSet<NodeId>;
    type Child = TopUniqueNodeSegmentCollector;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(TopUniqueNodeSegmentCollector {
            limit: self.limit,
            selector: self.selector,
            unique: HashSet::new(),
            store_reader: segment.get_store_reader(STORE_READER_CACHED_BLOCKS)?,
            schema: self.schema.clone(),
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

impl Collector for TopUniqueRelationCollector {
    type Fruit = Vec<TantivyDocument>;
    type Child = TopUniqueRelationSegmentCollector;

    fn requires_scoring(&self) -> bool {
        false
    }

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(TopUniqueRelationSegmentCollector {
            limit: self.limit,
            unique: HashMap::new(),
            store_reader: segment.get_store_reader(STORE_READER_CACHED_BLOCKS)?,
            schema: self.schema.clone(),
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut unique = HashSet::new();
        let mut docs = Vec::new();
        let mut fruits = segment_fruits.into_iter().flatten();
        let mut fruit = fruits.next();

        while fruit.is_some() && unique.len() < self.limit {
            let (relation_id, doc) = fruit.unwrap();
            if unique.insert(relation_id) {
                docs.push(doc);
            }
            fruit = fruits.next();
        }
        Ok(docs)
    }
}

impl SegmentCollector for TopUniqueNodeSegmentCollector {
    type Fruit = HashSet<NodeId>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        // we already have all unique results we need
        if self.unique.len() >= self.limit {
            return;
        }

        // log and skip documents not in the store. This should not happen
        let doc = match self.store_reader.get::<TantivyDocument>(doc_id) {
            Ok(doc) => doc,
            Err(error) => {
                warn!("Error while getting document from store: {error:?}");
                return;
            }
        };

        let node = match self.selector {
            NodeSelector::SourceNodes => {
                let source_value = self.schema.source_value(&doc).to_string();
                let source_type = io_maps::u64_to_node_type::<i32>(self.schema.source_type(&doc));
                let source_subtype = self.schema.source_subtype(&doc).to_string();
                (source_value, source_type, source_subtype)
            }
            NodeSelector::DestinationNodes => {
                let destination_value = self.schema.target_value(&doc).to_string();
                let destination_type = io_maps::u64_to_node_type::<i32>(self.schema.target_type(&doc));
                let destination_subtype = self.schema.target_subtype(&doc).to_string();
                (destination_value, destination_type, destination_subtype)
            }
        };
        self.unique.insert(node);
    }

    fn harvest(self) -> Self::Fruit {
        self.unique
    }
}

impl SegmentCollector for TopUniqueRelationSegmentCollector {
    type Fruit = HashMap<RelationId, TantivyDocument>;

    fn collect(&mut self, doc_id: DocId, _score: Score) {
        // we already have all unique results we need
        if self.unique.len() >= self.limit {
            return;
        }

        // log and skip documents not in the store. This should not happen
        let doc = match self.store_reader.get::<TantivyDocument>(doc_id) {
            Ok(doc) => doc,
            Err(error) => {
                warn!("Error while getting document from store: {error:?}");
                return;
            }
        };

        let relation_label = self.schema.relationship_label(&doc).to_string();
        let relation_type = self.schema.relationship(&doc);
        self.unique.insert((relation_label, relation_type), doc);
    }

    fn harvest(self) -> Self::Fruit {
        self.unique
    }
}
