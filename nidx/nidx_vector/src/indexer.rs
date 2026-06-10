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

use crate::config::{VectorCardinality, VectorConfig};
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
            let sentences_iterator = paragraphs_wrapper.paragraphs.values().filter_map(|paragraph| {
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

fn encode_metadata_field(rid: &str, field: &str) -> Option<Vec<u8>> {
    FieldKey::from_field_id(&format!("{rid}/{field}")).map(|k| k.bytes().to_vec())
}

pub fn index_relation_nodes(
    resource: &Resource,
    index_name: &str,
    output_path: &Path,
    config: &VectorConfig,
) -> anyhow::Result<Option<VectorSegmentMetadata>> {
    debug!("Creating elements for the main index");

    let Some(resource_id) = &resource.resource else {
        return Err(anyhow!("resource_id required"));
    };
    let rid = &resource_id.uuid;

    let mut elems = Vec::new();
    for (field_id, field_data) in &resource.field_node_vectors {
        let Some(vectorset) = field_data.node_vectors.get(index_name) else {
            continue;
        };
        let Some(metadata) = encode_metadata_field(rid, field_id) else {
            continue;
        };
        for node_vector in &vectorset.vectors {
            elems.push(Elem::new(
                node_vector.node_value.clone(),
                node_vector.vector.clone(),
                vec![],
                Some(metadata.clone()),
            ));
        }
    }

    if elems.is_empty() {
        return Ok(None);
    }

    debug!("Creating the segment");
    let segment = segment::create(output_path, elems, config, HashSet::new())?;

    Ok(Some(segment.into_metadata()))
}

pub fn index_relation_edges(
    resource: &Resource,
    index_name: &str,
    output_path: &Path,
    config: &VectorConfig,
) -> anyhow::Result<Option<VectorSegmentMetadata>> {
    debug!("Creating elements for the main index");

    let Some(resource_id) = &resource.resource else {
        return Err(anyhow!("resource_id required"));
    };
    let rid = &resource_id.uuid;

    let mut elems = Vec::new();
    for (field_id, field_data) in &resource.field_edge_vectors {
        let Some(vectorset) = field_data.edge_vectors.get(index_name) else {
            continue;
        };
        let Some(metadata) = encode_metadata_field(rid, field_id) else {
            continue;
        };
        for rel_vector in &vectorset.vectors {
            elems.push(Elem::new(
                rel_vector.relation_label.clone(),
                rel_vector.vector.clone(),
                vec![],
                Some(metadata.clone()),
            ));
        }
    }

    if elems.is_empty() {
        return Ok(None);
    }

    debug!("Creating the segment");
    let segment = segment::create(output_path, elems, config, HashSet::new())?;

    Ok(Some(segment.into_metadata()))
}
