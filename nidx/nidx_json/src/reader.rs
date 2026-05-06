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

use tantivy::collector::{Collector, SegmentCollector};
use tantivy::columnar::Column;
use tantivy::{Index, IndexReader, SegmentOrdinal, SegmentReader};
use uuid::Uuid;

use crate::schema::{JsonSchema, decode_rid};

pub struct JsonReaderService {
    pub index: Index,
    pub schema: JsonSchema,
    pub reader: IndexReader,
}

struct RidSegmentCollector {
    encoded_rid_reader: Column,
    results: HashSet<Uuid>,
}

impl SegmentCollector for RidSegmentCollector {
    type Fruit = HashSet<Uuid>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        let rid = decode_rid(self.encoded_rid_reader.values_for_doc(doc));
        self.results.insert(rid);
    }

    fn harvest(self) -> Self::Fruit {
        self.results
    }
}

struct RidCollector;

impl Collector for RidCollector {
    type Fruit = HashSet<Uuid>;
    type Child = RidSegmentCollector;

    fn for_segment(&self, _segment_local_id: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        let encoded_rid_reader = segment.fast_fields().u64("encoded_resource_id")?;
        Ok(RidSegmentCollector {
            encoded_rid_reader,
            results: HashSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(
        &self,
        segment_fruits: Vec<<Self::Child as SegmentCollector>::Fruit>,
    ) -> tantivy::Result<Self::Fruit> {
        Ok(segment_fruits.into_iter().flatten().collect())
    }
}

impl JsonReaderService {
    pub fn search(&self, query: &dyn tantivy::query::Query) -> anyhow::Result<HashSet<Uuid>> {
        let searcher = self.reader.searcher();
        Ok(searcher.search(query, &RidCollector)?)
    }

    pub fn space_usage(&self) -> usize {
        match self.reader.searcher().space_usage() {
            Ok(usage) => usage.total().get_bytes() as usize,
            Err(_) => 0,
        }
    }
}
