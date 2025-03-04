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
    fn segments(&self) -> impl Iterator<Item = (SegmentMetadata<VectorSegmentMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, nidx_types::Seq)> {
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
