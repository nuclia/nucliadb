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

use nidx_protos::ResourceId;

use crate::config::VectorConfig;
use crate::data_point::{self, DataPointPin, Elem, LabelDictionary};
use crate::data_point_provider::state::read_state;
use crate::data_point_provider::writer::Writer;
use crate::utils;
use nidx_protos::prost::*;
use nidx_protos::*;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::SystemTime;
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

    pub fn id(&self) -> &String {
        &self.resource.shard_id
    }

    pub fn labels(&self) -> &[String] {
        &self.resource.labels
    }

    pub fn fields(&self) -> impl Iterator<Item = (&String, impl Iterator<Item = ParagraphVectors>)> {
        self.resource.paragraphs.iter().map(|(field_id, paragraphs_wrapper)| {
            let sentences_iterator = paragraphs_wrapper.paragraphs.iter().filter_map(|(_paragraph_id, paragraph)| {
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

    pub fn sentences_to_delete(&self) -> impl Iterator<Item = &str> {
        self.resource.sentences_to_delete.iter().map(String::as_str)
    }
}

pub struct ParagraphVectors<'a> {
    pub vectors: &'a HashMap<String, noderesources::VectorSentence>,
    pub labels: &'a Vec<String>,
}

pub struct VectorWriterService {
    index: Writer,
    path: PathBuf,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

impl VectorWriterService {
    #[tracing::instrument(skip_all)]
    pub fn count(&self) -> anyhow::Result<usize> {
        Ok(self.index.size())
    }

    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> anyhow::Result<()> {
        let time = Instant::now();

        let id = Some(&resource_id.shard_id);
        let temporal_mark = SystemTime::now();
        let resource_uuid_bytes = resource_id.uuid.as_bytes();
        self.index.record_delete(resource_uuid_bytes, temporal_mark);
        self.index.commit()?;

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn set_resource(&mut self, resource: ResourceWrapper) -> anyhow::Result<()> {
        let time = Instant::now();

        let id = resource.id();
        debug!("{id:?} - Updating main index");
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating elements for the main index: starts {v} ms");

        let temporal_mark = SystemTime::now();
        let mut lengths: HashMap<usize, Vec<_>> = HashMap::new();
        let mut elems = Vec::new();
        let normalize_vectors = self.index.config().normalize_vectors;
        for (field_id, field_paragraphs) in resource.fields() {
            for paragraph in field_paragraphs {
                let mut inner_labels = paragraph.labels.clone();
                inner_labels.push(field_id.clone());
                let labels = LabelDictionary::new(inner_labels);

                for (key, sentence) in paragraph.vectors.iter().clone() {
                    let key = key.to_string();
                    let labels = labels.clone();
                    let vector = if normalize_vectors {
                        utils::normalize_vector(&sentence.vector)
                    } else {
                        sentence.vector.clone()
                    };
                    let metadata = sentence.metadata.as_ref().map(|m| m.encode_to_vec());
                    let bucket = lengths.entry(vector.len()).or_default();
                    elems.push(Elem::new(key, vector, labels, metadata));
                    bucket.push(field_id);
                }
            }
        }
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating elements for the main index: ends {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Main index set resource: starts {v} ms");

        if lengths.len() > 1 {
            return Ok(tracing::error!("{}", self.dimensions_report(lengths)));
        }

        if !elems.is_empty() {
            let tags = resource.labels().iter().filter(|t| SEGMENT_TAGS.contains(&t.as_str())).cloned().collect();
            let location = self.index.location();
            let time = Some(temporal_mark);
            let data_point_pin = DataPointPin::create_pin(location)?;
            data_point::create(&data_point_pin, elems, time, self.index.config(), tags)?;
            self.index.add_data_point(data_point_pin)?;
        }

        for to_delete in resource.sentences_to_delete() {
            let key_as_bytes = to_delete.as_bytes();
            self.index.record_delete(key_as_bytes, temporal_mark);
        }

        self.index.commit()?;

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Main index set resource: ends {v} ms");

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }
}

impl VectorWriterService {
    fn dimensions_report<'a>(&'a self, dimensions: HashMap<usize, Vec<&'a String>>) -> String {
        let mut report = String::new();
        for (dimension, bucket) in dimensions {
            let partial = format!("{dimension} : {bucket:?}\n");
            report.push_str(&partial);
        }
        report.pop();
        report
    }

    #[tracing::instrument(skip_all)]
    pub fn create(path: &Path, config: VectorConfig) -> anyhow::Result<Self> {
        if path.exists() {
            Err(anyhow::anyhow!("Shard does exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Writer::new(path, config)?,
                path: path.to_path_buf(),
            })
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn open(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            Err(anyhow::anyhow!("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Writer::open(path)?,
                path: path.to_path_buf(),
            })
        }
    }

    pub fn get_segment_ids(&self) -> anyhow::Result<Vec<String>> {
        let state = read_state(File::open(self.path.join("state.bincode"))?)?;
        Ok(state.data_point_list.iter().map(|d| d.to_string()).collect())
    }
}

#[cfg(test)]
mod tests {
    use nidx_protos::resource::ResourceStatus;
    use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence, VectorsetSentences};
    use std::collections::HashMap;
    use tempfile::TempDir;

    use crate::config::{Similarity, VectorType};

    use super::*;

    #[test]
    fn test_new_vector_writer() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 {
                dimension: 3,
            },
        };
        let raw_sentences = [
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
        ];
        let resource_id = ResourceId {
            shard_id: "DOC".to_string(),
            uuid: "DOC/KEY".to_string(),
        };

        let mut sentences = HashMap::new();
        for (key, vector) in raw_sentences {
            let vector = VectorSentence {
                vector,
                ..Default::default()
            };
            sentences.insert(key, vector);
        }
        let paragraph = IndexParagraph {
            start: 0,
            end: 0,
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([(
                "__default__".to_string(),
                VectorsetSentences {
                    sentences,
                },
            )]),
            field: "".to_string(),
            labels: vec!["1".to_string(), "2".to_string(), "3".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let paragraphs = IndexParagraphs {
            paragraphs: HashMap::from([("DOC/KEY/1".to_string(), paragraph)]),
        };
        let resource = Resource {
            resource: Some(resource_id.clone()),
            metadata: None,
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["FULL".to_string()],
            paragraphs: HashMap::from([("DOC/KEY".to_string(), paragraphs)]),
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };
        // insert - delete - insert sequence
        let mut writer = VectorWriterService::create(&dir.path().join("vectors"), vsc).unwrap();
        let res = writer.set_resource(ResourceWrapper::from(&resource));
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(ResourceWrapper::from(&resource));
        assert!(res.is_ok());
    }

    #[test]
    fn test_get_segments() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 {
                dimension: 3,
            },
        };
        let resource_id = ResourceId {
            shard_id: "DOC".to_string(),
            uuid: "DOC/KEY".to_string(),
        };

        let mut sentences = HashMap::new();
        sentences.insert(
            "DOC/KEY/1/1".to_string(),
            VectorSentence {
                vector: vec![1.0, 3.0, 4.0],
                ..Default::default()
            },
        );

        let paragraph = IndexParagraph {
            start: 0,
            end: 0,
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([(
                "__default__".to_string(),
                VectorsetSentences {
                    sentences,
                },
            )]),
            field: "".to_string(),
            labels: vec!["1".to_string(), "2".to_string(), "3".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let paragraphs = IndexParagraphs {
            paragraphs: HashMap::from([("DOC/KEY/1".to_string(), paragraph)]),
        };
        let resource = Resource {
            resource: Some(resource_id.clone()),
            metadata: None,
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["FULL".to_string()],
            paragraphs: HashMap::from([("DOC/KEY".to_string(), paragraphs)]),
            paragraphs_to_delete: vec![],
            sentences_to_delete: vec![],
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };
        // insert - delete - insert sequence
        let mut writer = VectorWriterService::create(&dir.path().join("vectors"), vsc).unwrap();
        let res = writer.set_resource(ResourceWrapper::from(&resource));
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(ResourceWrapper::from(&resource));
        assert!(res.is_ok());

        let segments = writer.get_segment_ids().unwrap();
        assert_eq!(segments.len(), 2);
    }
}
