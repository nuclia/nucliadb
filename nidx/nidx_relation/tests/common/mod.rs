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
    fn segments(&self) -> impl Iterator<Item = (nidx_types::SegmentMetadata<TantivyMeta>, nidx_types::Seq)> {
        self.segments.iter().cloned()
    }

    fn deletions(&self) -> impl Iterator<Item = (&String, nidx_types::Seq)> {
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
