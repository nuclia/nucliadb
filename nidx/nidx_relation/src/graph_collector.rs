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

use tantivy::{collector::{Collector, SegmentCollector}, store::StoreReader, DocAddress, DocId, Score, SegmentOrdinal, SegmentReader, TantivyDocument};

use crate::io_maps;

/// TODO
pub struct TopUniqueNodesCollector {
    // TODO: remove pubs and add constructor
    pub limit: usize,
}

pub struct TopUniqueNodesSegmentCollector {
    limit: usize,
    unique_nodes: HashSet<(String, i32, String)>,
    store_reader: StoreReader,
    schema: crate::schema::Schema,
}

impl Collector for TopUniqueNodesCollector {
    type Fruit = HashSet<(String, i32, String)>;
    type Child = TopUniqueNodesSegmentCollector;

    fn requires_scoring(&self) -> bool {
        true
    }

    fn for_segment(
        &self,
        _segment_local_id: SegmentOrdinal,
        segment: &SegmentReader,
    ) -> tantivy::Result<Self::Child> {
        // TODO
        let store = segment.get_store_reader(100)?;

        Ok(TopUniqueNodesSegmentCollector {
            limit: self.limit,
            unique_nodes: HashSet::new(),
            store_reader: store,
            schema: crate::schema::Schema::new(),
        })
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        let mut unique_nodes = HashSet::new();
        for fruits in segment_fruits {
            unique_nodes.extend(fruits.into_iter());
        }
        Ok(unique_nodes)
    }
}

impl SegmentCollector for TopUniqueNodesSegmentCollector {
    type Fruit = HashSet<(String, i32, String)>;

    fn collect(&mut self, doc: DocId, score: Score) {
        if let Ok(doc) = self.store_reader.get::<TantivyDocument>(doc) {
            let source_value = self.schema.source_value(&doc).to_string();
            let source_type = io_maps::u64_to_node_type::<i32>(self.schema.source_type(&doc));
            let source_subtype = self.schema.source_subtype(&doc).to_string();

            let destination_value = self.schema.target_value(&doc).to_string();
            let destination_type = io_maps::u64_to_node_type::<i32>(self.schema.target_type(&doc));
            let destination_subtype = self.schema.target_subtype(&doc).to_string();

            self.unique_nodes.insert((source_value, source_type, source_subtype));
            self.unique_nodes.insert((destination_value, destination_type, destination_subtype));
        } else {
            println!("ERROR while getting doc {doc:?} from store");
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.unique_nodes
    }
}
