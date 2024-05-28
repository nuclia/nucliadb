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

use crate::config::VectorConfig;
use crate::data_point::{self, DataPointPin, Elem, LabelDictionary};
use crate::data_point_provider::garbage_collector;
use crate::data_point_provider::writer::Writer;
use crate::data_point_provider::*;
use crate::utils;
use nucliadb_core::metrics;
use nucliadb_core::metrics::request_time;
use nucliadb_core::metrics::vectors::MergeSource;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::ResourceId;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::MergeMetrics;
use nucliadb_core::vectors::*;
use nucliadb_core::{IndexFiles, RawReplicaState};
use nucliadb_procs::measure;
use std::collections::HashMap;
use std::fmt::Debug;
use std::path::{Path, PathBuf};
use std::time::Instant;
use std::time::SystemTime;

pub struct VectorWriterService {
    index: Writer,
    path: PathBuf,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

impl VectorWriter for VectorWriterService {
    #[measure(actor = "vectors", metric = "force_garbage_collection")]
    #[tracing::instrument(skip_all)]
    fn force_garbage_collection(&mut self) -> NodeResult<()> {
        Ok(())
    }

    #[measure(actor = "vectors", metric = "prepare_merge")]
    #[tracing::instrument(skip_all)]
    fn prepare_merge(&self, parameters: MergeParameters) -> NodeResult<Option<Box<dyn MergeRunner>>> {
        Ok(self.index.prepare_merge(parameters)?)
    }

    #[measure(actor = "vectors", metric = "record_merge")]
    #[tracing::instrument(skip_all)]
    fn record_merge(&mut self, merge_result: Box<dyn MergeResults>, source: MergeSource) -> NodeResult<MergeMetrics> {
        self.index.record_merge(merge_result.as_ref())?;
        merge_result.record_metrics(source);
        Ok(merge_result.get_metrics())
    }

    #[measure(actor = "vectors", metric = "reload")]
    #[tracing::instrument(skip_all)]
    fn reload(&mut self) -> NodeResult<()> {
        Ok(self.index.reload()?)
    }

    #[measure(actor = "vectors", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        Ok(self.index.size())
    }

    #[measure(actor = "vectors", metric = "delete_resource")]
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
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

    #[measure(actor = "vectors", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: nucliadb_core::vectors::ResourceWrapper) -> NodeResult<()> {
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
            let location = self.index.location();
            let time = Some(temporal_mark);
            let data_point_pin = DataPointPin::create_pin(location)?;
            data_point::create(&data_point_pin, elems, time, self.index.config())?;
            self.index.add_data_point(data_point_pin)?;
        }

        for to_delete in resource.sentences_to_delete() {
            let key_as_bytes = to_delete.as_bytes();
            self.index.record_delete(key_as_bytes, temporal_mark);
        }

        self.index.commit()?;

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Main index set resource: ends {v} ms");

        let metrics = metrics::get_metrics();
        let took = time.elapsed().as_secs_f64();
        let metric = request_time::RequestTimeKey::vectors("set_resource".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "garbage_collection")]
    #[tracing::instrument(skip_all)]
    fn garbage_collection(&mut self) -> NodeResult<()> {
        let time = Instant::now();

        garbage_collector::collect_garbage(self.index.location())?;

        let took = time.elapsed().as_secs_f64();
        debug!("Garbage collection {took} ms");
        Ok(())
    }

    fn get_segment_ids(&self) -> NodeResult<Vec<String>> {
        Ok(replication::get_segment_ids(&self.path)?)
    }

    fn get_index_files(&self, prefix: &str, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        // Should be called along with a lock at a higher level to be safe
        let replica_state = replication::get_index_files(&self.path, prefix, ignored_segment_ids)?;

        if replica_state.files.is_empty() {
            // exit with no changes
            return Ok(IndexFiles::Other(RawReplicaState::default()));
        }

        Ok(IndexFiles::Other(replica_state))
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
    pub fn create(path: &Path, shard_id: String, config: VectorConfig) -> NodeResult<Self> {
        if path.exists() {
            Err(node_error!("Shard does exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Writer::new(path, config, shard_id)?,
                path: path.to_path_buf(),
            })
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn open(path: &Path, shard_id: String) -> NodeResult<Self> {
        if !path.exists() {
            Err(node_error!("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Writer::open(path, shard_id)?,
                path: path.to_path_buf(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence, VectorsetSentences,
    };
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
        let mut writer = VectorWriterService::create(&dir.path().join("vectors"), "abc".into(), vsc).unwrap();
        let res = writer.set_resource(nucliadb_core::vectors::ResourceWrapper::from(&resource));
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(nucliadb_core::vectors::ResourceWrapper::from(&resource));
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
        let mut writer = VectorWriterService::create(&dir.path().join("vectors"), "abc".into(), vsc).unwrap();
        let res = writer.set_resource(nucliadb_core::vectors::ResourceWrapper::from(&resource));
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(nucliadb_core::vectors::ResourceWrapper::from(&resource));
        assert!(res.is_ok());

        let segments = writer.get_segment_ids().unwrap();
        assert_eq!(segments.len(), 2);
        let existing_secs: Vec<String> = Vec::new();
        let Ok(IndexFiles::Other(index_files)) = writer.get_index_files("vectors", &existing_secs) else {
            panic!("Expected another outcome");
        };
        let mut expected_files = Vec::new();
        for segment in segments {
            expected_files.push(format!("vectors/{}/index.hnsw", segment));
            expected_files.push(format!("vectors/{}/journal.json", segment));
            expected_files.push(format!("vectors/{}/nodes.kv", segment));
        }
        assert_eq!(index_files.files.into_iter().map(|x| x.0).collect::<Vec<_>>(), expected_files);
        assert_eq!(index_files.metadata_files.len(), 2);
    }
}
