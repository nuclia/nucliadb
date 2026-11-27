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

use std::collections::{BTreeSet, HashSet};

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

struct RidPageCollector {
    after: Option<Uuid>,
    limit: usize,
}

struct RidPageSegmentCollector {
    encoded_rid_reader: Column,
    after: Option<Uuid>,
    limit: usize,
    results: BTreeSet<Uuid>,
}

impl SegmentCollector for RidPageSegmentCollector {
    type Fruit = BTreeSet<Uuid>;

    fn collect(&mut self, doc: tantivy::DocId, _score: tantivy::Score) {
        let rid = decode_rid(self.encoded_rid_reader.values_for_doc(doc));
        if self.after.is_none_or(|after| rid > after) {
            self.results.insert(rid);
            if self.results.len() > self.limit {
                self.results.pop_last();
            }
        }
    }

    fn harvest(self) -> Self::Fruit {
        self.results
    }
}

impl Collector for RidPageCollector {
    type Fruit = Vec<Uuid>;
    type Child = RidPageSegmentCollector;

    fn for_segment(&self, _ordinal: SegmentOrdinal, segment: &SegmentReader) -> tantivy::Result<Self::Child> {
        Ok(RidPageSegmentCollector {
            encoded_rid_reader: segment.fast_fields().u64("encoded_resource_id")?,
            after: self.after,
            limit: self.limit,
            results: BTreeSet::new(),
        })
    }

    fn requires_scoring(&self) -> bool {
        false
    }

    fn merge_fruits(&self, fruits: Vec<BTreeSet<Uuid>>) -> tantivy::Result<Self::Fruit> {
        let mut results = BTreeSet::new();
        for fruit in fruits {
            for rid in fruit {
                results.insert(rid);
                if results.len() > self.limit {
                    results.pop_last();
                }
            }
        }
        Ok(results.into_iter().collect())
    }
}

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
    pub fn search_page(
        &self,
        query: &dyn tantivy::query::Query,
        after: Option<Uuid>,
        limit: usize,
    ) -> anyhow::Result<Vec<Uuid>> {
        anyhow::ensure!(limit > 0, "page limit must be positive");
        Ok(self
            .reader
            .searcher()
            .search(query, &RidPageCollector { after, limit })?)
    }

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
