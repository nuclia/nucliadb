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

#![allow(dead_code)] // clippy doesn't check for usage in other tests modules

use nidx_paragraph::{ParagraphIndexer, ParagraphSearcher};
use nidx_protos::Resource;
use nidx_tantivy::{TantivyMeta, TantivySegmentMetadata};
use nidx_types::{OpenIndexMetadata, Seq};
use tempfile::TempDir;

pub struct TestOpener {
    segments: Vec<(TantivySegmentMetadata, Seq)>,
    deletions: Vec<(String, Seq)>,
}

impl TestOpener {
    pub fn new(segments: Vec<(TantivySegmentMetadata, Seq)>, deletions: Vec<(String, Seq)>) -> Self {
        Self { segments, deletions }
    }
}

impl OpenIndexMetadata<TantivyMeta> for TestOpener {
    fn segments(&self) -> impl DoubleEndedIterator<Item = (nidx_types::SegmentMetadata<TantivyMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl DoubleEndedIterator<Item = (&String, nidx_types::Seq)> {
        self.deletions.iter().map(|(key, seq)| (key, *seq))
    }
}

pub fn test_reader(resource: &Resource) -> ParagraphSearcher {
    let dir = TempDir::new().unwrap();
    let segment_meta = ParagraphIndexer.index_resource(dir.path(), resource).unwrap().unwrap();

    ParagraphSearcher::open(TestOpener::new(vec![(segment_meta, 1i64.into())], vec![])).unwrap()
}
