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

use nidx_protos::relation::RelationType;
use nidx_protos::relation_node::NodeType;
use nidx_protos::{IndexRelation, Relation, RelationMetadata, RelationNode};
use nidx_tantivy::{TantivyMeta, TantivySegmentMetadata};
use nidx_types::{OpenIndexMetadata, Seq};

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

pub fn create_relation_node(source: String, node_type: NodeType, subtype: String) -> RelationNode {
    RelationNode {
        subtype,
        value: source,
        ntype: node_type.into(),
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create_relation(
    source: String,
    source_node_type: NodeType,
    source_subtype: String,
    to: String,
    to_node_type: NodeType,
    to_subtype: String,
    rel_type: RelationType,
    rel_label: String,
) -> IndexRelation {
    IndexRelation {
        relation: Some(Relation {
            source: Some(create_relation_node(source, source_node_type, source_subtype)),
            to: Some(create_relation_node(to, to_node_type, to_subtype)),
            relation: rel_type.into(),
            relation_label: rel_label,
            metadata: Some(RelationMetadata { ..Default::default() }),
        }),
        ..Default::default()
    }
}

#[allow(clippy::too_many_arguments)]
pub fn create_relation_with_metadata(
    source: String,
    source_node_type: NodeType,
    source_subtype: String,
    to: String,
    to_node_type: NodeType,
    to_subtype: String,
    rel_type: RelationType,
    rel_label: String,
    metadata: RelationMetadata,
) -> IndexRelation {
    let mut relation = create_relation(
        source,
        source_node_type,
        source_subtype,
        to,
        to_node_type,
        to_subtype,
        rel_type,
        rel_label,
    );
    relation.relation.as_mut().unwrap().metadata = Some(metadata);
    relation
}
