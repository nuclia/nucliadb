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

use std::collections::HashMap;

use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence};
use nidx_types::{OpenIndexMetadata, SegmentMetadata, Seq};
use nidx_vector::VectorSegmentMeta;
use uuid::Uuid;

pub struct TestOpener {
    segments: Vec<(SegmentMetadata<VectorSegmentMeta>, Seq)>,
    deletions: Vec<(String, Seq)>,
}

impl TestOpener {
    pub fn new(segments: Vec<(SegmentMetadata<VectorSegmentMeta>, Seq)>, deletions: Vec<(String, Seq)>) -> Self {
        Self { segments, deletions }
    }
}

impl OpenIndexMetadata<VectorSegmentMeta> for TestOpener {
    fn segments(&self) -> impl DoubleEndedIterator<Item = (SegmentMetadata<VectorSegmentMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl DoubleEndedIterator<Item = (&String, nidx_types::Seq)> {
        self.deletions.iter().map(|(key, seq)| (key, *seq))
    }
}

pub fn resource(resource_labels: Vec<String>, paragraph_labels: Vec<String>) -> Resource {
    let id = Uuid::new_v4().to_string();
    Resource {
        resource: Some(ResourceId {
            shard_id: String::new(),
            uuid: id.clone(),
        }),
        labels: resource_labels,
        paragraphs: HashMap::from([(
            format!("{id}/a/title"),
            IndexParagraphs {
                paragraphs: HashMap::from([(
                    format!("{id}/a/title/0-5"),
                    IndexParagraph {
                        start: 0,
                        end: 5,
                        labels: paragraph_labels,
                        sentences: HashMap::from([(
                            format!("{id}/a/title/0-5"),
                            VectorSentence {
                                vector: vec![0.5, 0.5, 0.5, rand::random()],
                                metadata: None,
                            },
                        )]),
                        ..Default::default()
                    },
                )]),
            },
        )]),
        ..Default::default()
    }
}
