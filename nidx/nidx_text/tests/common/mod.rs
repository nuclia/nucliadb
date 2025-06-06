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

use std::collections::HashMap;
use std::time::SystemTime;

use nidx_protos::prost_types::Timestamp;
use nidx_protos::{Resource, ResourceId};
use nidx_tantivy::{TantivyMeta, TantivySegmentMetadata};
use nidx_text::{TextConfig, TextIndexer, TextSearcher};
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

pub fn test_reader() -> TextSearcher {
    let dir = TempDir::new().unwrap();
    let resource = create_resource("shard".to_string());
    let segment_meta = TextIndexer
        .index_resource(dir.path(), TextConfig::default(), &resource)
        .unwrap()
        .unwrap();

    TextSearcher::open(
        TextConfig::default(),
        TestOpener::new(vec![(segment_meta, 1i64.into())], vec![]),
    )
    .unwrap()
}

pub fn create_resource(shard_id: String) -> Resource {
    let resource_id = ResourceId {
        shard_id: shard_id.to_string(),
        uuid: "f56c58ac-b4f9-4d61-a077-ffccaadd0001".to_string(),
    };

    let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap();
    let timestamp = Timestamp {
        seconds: now.as_secs() as i64,
        nanos: 0,
    };

    let metadata = nidx_protos::IndexMetadata {
        created: Some(timestamp),
        modified: Some(timestamp),
    };

    const DOC1_TI: &str = "This is the first document";
    const DOC1_P1: &str = "This is the text of the second paragraph.";
    const DOC1_P2: &str = "This should be enough to test the tantivy.";
    const DOC1_P3: &str = "But I wanted to make it three anyway.";

    let ti_title = nidx_protos::TextInformation {
        text: DOC1_TI.to_string(),
        labels: vec!["/l/mylabel".to_string(), "/e/myentity".to_string()],
    };

    let ti_body = nidx_protos::TextInformation {
        text: DOC1_P1.to_string() + DOC1_P2 + DOC1_P3,
        labels: vec!["/f/body".to_string(), "/l/mylabel2".to_string()],
    };

    let mut texts = HashMap::new();
    texts.insert("a/title".to_string(), ti_title);
    texts.insert("a/body".to_string(), ti_body);

    Resource {
        resource: Some(resource_id),
        metadata: Some(metadata),
        texts,
        status: nidx_protos::resource::ResourceStatus::Processed as i32,
        labels: vec![],
        paragraphs: HashMap::new(),
        paragraphs_to_delete: vec![],
        vectors: HashMap::default(),
        vectors_to_delete: HashMap::default(),
        shard_id,
        ..Default::default()
    }
}
