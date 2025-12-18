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

use crate::config::{VectorCardinality, VectorConfig};
use crate::field_list_metadata::encode_field_list_metadata;
use crate::multivector::extract_multi_vectors;
use crate::segment::{self, Elem};
use crate::utils::FieldKey;
use crate::{VectorSegmentMetadata, utils};
use anyhow::anyhow;
use nidx_protos::{Resource, noderesources, prost::*};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use tracing::*;

pub const SEGMENT_TAGS: &[&str] = &["/q/h"];

pub struct ResourceWrapper<'a> {
    resource: &'a noderesources::Resource,
    vectorset: Option<String>,
    fallback_to_default_vectorset: bool,
}

impl<'a> From<&'a noderesources::Resource> for ResourceWrapper<'a> {
    fn from(value: &'a noderesources::Resource) -> Self {
        Self {
            resource: value,
            vectorset: None,
            fallback_to_default_vectorset: false,
        }
    }
}

impl<'a> ResourceWrapper<'a> {
    pub fn new_vectorset_resource(
        resource: &'a noderesources::Resource,
        vectorset: &str,
        fallback_to_default_vectorset: bool,
    ) -> Self {
        Self {
            resource,
            vectorset: Some(vectorset.to_string()),
            fallback_to_default_vectorset,
        }
    }

    pub fn labels(&self) -> &[String] {
        &self.resource.labels
    }

    pub fn fields(&self) -> impl Iterator<Item = (&String, impl Iterator<Item = ParagraphVectors<'_>>)> {
        self.resource.paragraphs.iter().map(|(field_id, paragraphs_wrapper)| {
            let sentences_iterator = paragraphs_wrapper
                .paragraphs
                .iter()
                .filter_map(|(_paragraph_id, paragraph)| {
                    let sentences = if let Some(vectorset) = &self.vectorset {
                        // indexing a vectorset, we should return only paragraphs from this vectorset.
                        // If vectorset is not found, we'll skip this paragraph
                        if let Some(vectorset_sentences) = paragraph.vectorsets_sentences.get(vectorset) {
                            Some(&vectorset_sentences.sentences)
                        } else if self.fallback_to_default_vectorset {
                            Some(&paragraph.sentences)
                        } else {
                            None
                        }
                    } else {
                        // Default vectors index (no vectorset)
                        Some(&paragraph.sentences)
                    };
                    sentences.map(|s| ParagraphVectors {
                        vectors: s,
                        labels: &paragraph.labels,
                    })
                });
            (field_id, sentences_iterator)
        })
    }
}

pub struct ParagraphVectors<'a> {
    pub vectors: &'a HashMap<String, noderesources::VectorSentence>,
    pub labels: &'a Vec<String>,
}

pub fn index_resource(
    resource: ResourceWrapper,
    output_path: &Path,
    config: &VectorConfig,
) -> anyhow::Result<Option<VectorSegmentMetadata>> {
    debug!("Creating elements for the main index");

    let mut elems = Vec::new();
    let normalize_vectors = config.normalize_vectors;
    for (_, field_paragraphs) in resource.fields() {
        for paragraph in field_paragraphs {
            for (key, sentence) in paragraph.vectors.iter().clone() {
                let key = key.to_string();
                let vector = if normalize_vectors {
                    utils::normalize_vector(&sentence.vector)
                } else {
                    sentence.vector.clone()
                };
                let metadata = sentence.metadata.as_ref().map(|m| m.encode_to_vec());

                match config.vector_cardinality {
                    VectorCardinality::Single => elems.push(Elem::new(key, vector, paragraph.labels.clone(), metadata)),
                    VectorCardinality::Multi => {
                        let vectors = extract_multi_vectors(&vector, &config.vector_type)?;
                        elems.push(Elem::new_multivector(
                            key.clone(),
                            vectors,
                            paragraph.labels.clone(),
                            metadata.clone(),
                        ));
                    }
                };
            }
        }
    }

    if elems.is_empty() {
        return Ok(None);
    }

    let tags = resource
        .labels()
        .iter()
        .filter(|t| SEGMENT_TAGS.contains(&t.as_str()))
        .cloned()
        .collect();

    debug!("Creating the segment");
    let segment = segment::create(output_path, elems, config, tags)?;

    Ok(Some(segment.into_metadata()))
}

fn encode_metadata_field(rid: &str, fields: &HashSet<&String>) -> Vec<u8> {
    let encoded_fields: Vec<_> = fields
        .iter()
        .map(|f| format!("{rid}/{f}"))
        .filter_map(|f| FieldKey::from_field_id(&f))
        .collect();
    encode_field_list_metadata(&encoded_fields)
}

pub fn index_relations(
    resource: &Resource,
    output_path: &Path,
    config: &VectorConfig,
) -> anyhow::Result<Option<VectorSegmentMetadata>> {
    debug!("Creating elements for the main index");

    let mut entity_fields = HashMap::new();
    let Some(resource_id) = &resource.resource else {
        return Err(anyhow!("resource_id required"));
    };
    let rid = &resource_id.uuid;

    for (field, relations) in &resource.field_relations {
        for relation in &relations.relations {
            let Some(relation) = &relation.relation else {
                return Err(anyhow!("relation required"));
            };
            let Some(source) = &relation.source else {
                return Err(anyhow!("relation source node required"));
            };
            entity_fields
                .entry(source.value.clone())
                .or_insert_with(HashSet::new)
                .insert(field);

            let Some(to) = &relation.to else {
                return Err(anyhow!("relation to node required"));
            };
            entity_fields
                .entry(to.value.clone())
                .or_insert_with(HashSet::new)
                .insert(field);
        }
    }

    let mut elems = Vec::new();
    for node_vector in &resource.relation_node_vectors {
        let vector = node_vector.vector.clone();
        let Some(node) = &node_vector.node else {
            return Err(anyhow!("relation node required"));
        };
        let fields = entity_fields.get(&node.value);
        let Some(fields) = fields else {
            continue;
        };
        elems.push(Elem::new(
            node.value.clone(),
            vector,
            vec![],
            Some(encode_metadata_field(rid, fields)),
        ));
    }

    if elems.is_empty() {
        return Ok(None);
    }

    debug!("Creating the segment");
    let segment = segment::create(output_path, elems, config, HashSet::new())?;

    Ok(Some(segment.into_metadata()))
}
