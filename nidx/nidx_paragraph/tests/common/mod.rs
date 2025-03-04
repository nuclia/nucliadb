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
    fn segments(&self) -> impl Iterator<Item = (nidx_types::SegmentMetadata<TantivyMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, nidx_types::Seq)> {
        self.deletions.iter().map(|(key, seq)| (key, *seq))
    }
}

pub fn test_reader(resource: &Resource) -> ParagraphSearcher {
    let dir = TempDir::new().unwrap();
    let segment_meta = ParagraphIndexer.index_resource(dir.path(), resource).unwrap().unwrap();

    ParagraphSearcher::open(TestOpener::new(vec![(segment_meta, 1i64.into())], vec![])).unwrap()
}
