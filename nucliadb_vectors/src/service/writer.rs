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

use crate::data_point::{self, DataPointPin, Elem, LabelDictionary};
use crate::data_point_provider::garbage_collector;
use crate::data_point_provider::writer::MergeParameters;
use crate::data_point_provider::writer::Writer;
use crate::data_point_provider::*;
use crate::indexset::IndexKeyCollector;
use crate::indexset::WriterSet;
use nucliadb_core::metrics::{get_metrics, request_time, vectors::MergeSource};
use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::MergeMetrics;
use nucliadb_core::vectors::*;
use nucliadb_core::{metrics, IndexFiles, RawReplicaState};
use nucliadb_procs::measure;
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::time::Instant;
use std::time::SystemTime;

impl IndexKeyCollector for Vec<String> {
    fn add_key(&mut self, key: String) {
        self.push(key);
    }
}

pub struct VectorWriterService {
    index: Writer,
    indexset: WriterSet,
    config: VectorConfig,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

fn record_merge_metrics(source: MergeSource, data: &crate::data_point_provider::writer::MergeMetrics) {
    if data.merged == 0 {
        return;
    }

    let metrics = &get_metrics().vectors_metrics;
    metrics.record_time(source, data.seconds_elapsed);
    for input in &data.input_segment_sizes {
        metrics.record_input_segment(source, *input);
    }
    metrics.record_output_segment(source, data.output_segment_size);
}

impl VectorWriter for VectorWriterService {
    #[measure(actor = "vectors", metric = "force_garbage_collection")]
    #[tracing::instrument(skip_all)]
    fn force_garbage_collection(&mut self) -> NodeResult<()> {
        Ok(())
    }

    #[measure(actor = "vectors", metric = "merge")]
    #[tracing::instrument(skip_all)]
    fn merge(&mut self, context: MergeContext) -> NodeResult<MergeMetrics> {
        let time = Instant::now();
        let merge_on_demand_parameters = MergeParameters {
            max_nodes_in_merge: context.max_nodes_in_merge,
            segments_before_merge: context.segments_before_merge,
        };
        let inner_metrics = self.index.merge(merge_on_demand_parameters)?;
        let took = time.elapsed().as_secs_f64();
        debug!("Merge took: {took} s");
        record_merge_metrics(context.source, &inner_metrics);

        Ok(MergeMetrics {
            merged: inner_metrics.merged,
            left: inner_metrics.segments_left,
        })
    }

    #[measure(actor = "vectors", metric = "list_vectorsets")]
    #[tracing::instrument(skip_all)]
    fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let time = Instant::now();

        let id: Option<String> = None;
        let mut collector = Vec::new();
        self.indexset.index_keys(&mut collector);

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(collector)
    }

    #[measure(actor = "vectors", metric = "add_vectorset")]
    #[tracing::instrument(skip_all)]
    fn add_vectorset(&mut self, setid: &VectorSetId, similarity: VectorSimilarity) -> NodeResult<()> {
        let time = Instant::now();

        let id = setid.shard.as_ref().map(|s| &s.id);
        let set = &setid.vectorset;
        let indexid = setid.vectorset.as_str();
        let similarity = similarity.into();
        self.indexset.create_index(indexid, similarity)?;
        self.indexset.commit()?;

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?}/{set} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "remove_vectorset")]
    #[tracing::instrument(skip_all)]
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()> {
        let time = Instant::now();

        let id = setid.shard.as_ref().map(|s| &s.id);
        let set = &setid.vectorset;
        let index_id = &setid.vectorset;
        self.indexset.remove_index(index_id);
        self.indexset.commit()?;

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?}/{set} - Ending at {took} ms");

        Ok(())
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
        let merge_metrics = self.index.commit()?;
        record_merge_metrics(MergeSource::OnCommit, &merge_metrics);

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let time = Instant::now();

        let id = resource.resource.as_ref().map(|i| &i.shard_id);
        debug!("{id:?} - Updating main index");
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating elements for the main index: starts {v} ms");

        let temporal_mark = SystemTime::now();
        let mut lengths: HashMap<usize, Vec<_>> = HashMap::new();
        let mut elems = Vec::new();
        if resource.status != ResourceStatus::Delete as i32 {
            for (paragraph_field, paragraph) in resource.paragraphs.iter() {
                for index in paragraph.paragraphs.values() {
                    let mut inner_labels = index.labels.clone();
                    inner_labels.push(paragraph_field.clone());
                    let labels = LabelDictionary::new(inner_labels);

                    for (key, sentence) in index.sentences.iter().clone() {
                        let key = key.to_string();
                        let labels = labels.clone();
                        let vector = sentence.vector.clone();
                        let metadata = sentence.metadata.as_ref().map(|m| m.encode_to_vec());
                        let bucket = lengths.entry(vector.len()).or_default();
                        elems.push(Elem::new(key, vector, labels, metadata));
                        bucket.push(paragraph_field);
                    }
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
            let similarity = self.index.metadata().similarity;
            let data_point_pin = DataPointPin::create_pin(location)?;
            data_point::create(&data_point_pin, elems, time, similarity)?;
            self.index.add_data_point(data_point_pin)?;
        }

        for to_delete in &resource.sentences_to_delete {
            let key_as_bytes = to_delete.as_bytes();
            self.index.record_delete(key_as_bytes, temporal_mark);
        }

        let merge_metrics = self.index.commit()?;
        record_merge_metrics(MergeSource::OnCommit, &merge_metrics);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Main index set resource: ends {v} ms");

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Set resource in vector sets: starts {v} ms");

        let mut writer_sets = HashMap::new();
        for (index_key, vectors_to_delete) in resource.vectors_to_delete.iter() {
            let Some(mut writer) = self.indexset.get(index_key)? else {
                continue;
            };

            for vector_key in &vectors_to_delete.vectors {
                writer.record_delete(vector_key.as_bytes(), temporal_mark);
            }

            writer_sets.insert(index_key.clone(), writer);
        }

        for (index_key, vectors_to_add) in resource.vectors.iter() {
            if !writer_sets.contains_key(index_key) {
                let Some(writer) = self.indexset.get(index_key)? else {
                    continue;
                };
                writer_sets.insert(index_key.clone(), writer);
            }

            let writer = writer_sets.get_mut(index_key).unwrap();
            let mut lengths: HashMap<usize, Vec<_>> = HashMap::new();
            let mut elems = Vec::new();
            for (vector_key, vector_data) in &vectors_to_add.vectors {
                let vector = vector_data.vector.clone();
                let bucket = lengths.entry(vector.len()).or_default();
                let labels = LabelDictionary::new(vector_data.labels.clone());
                elems.push(Elem::new(vector_key.clone(), vector, labels, None));
                bucket.push(vector_key.clone());
            }
            if !elems.is_empty() {
                let similarity = writer.metadata().similarity;
                let location = writer.location();
                let new_data_point_pin = DataPointPin::create_pin(location)?;
                data_point::create(&new_data_point_pin, elems, Some(temporal_mark), similarity)?;
                writer.add_data_point(new_data_point_pin)?;
            }
        }

        for (_, mut writer) in writer_sets {
            let merge_metrics = writer.commit()?;
            record_merge_metrics(MergeSource::OnCommit, &merge_metrics);
        }

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Set resource in vector sets: ends {v} ms");

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
        let mut segment_ids = replication::get_segment_ids(&self.config.path)?;
        for vs in self.list_vectorsets()? {
            segment_ids.extend(replication::get_segment_ids(&self.config.vectorset.join(vs))?);
        }
        Ok(segment_ids)
    }

    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        // Should be called along with a lock at a higher level to be safe
        let mut replica_state = replication::get_index_files(&self.config.path, "vectors", ignored_segment_ids)?;

        let vectorsets = self.list_vectorsets()?;
        if !vectorsets.is_empty() {
            replica_state
                .metadata_files
                .insert("vectorset/state.bincode".to_string(), fs::read(self.config.vectorset.join("state.bincode"))?);
            for vs in vectorsets {
                let vectorset_replica_state = replication::get_index_files(
                    &self.config.vectorset.join(&vs),
                    &format!("vectorset/{vs}"),
                    ignored_segment_ids,
                )?;
                replica_state.extend(vectorset_replica_state);
            }
        }

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
    pub fn start(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            match VectorWriterService::new(config) {
                Err(e) if path.exists() => {
                    std::fs::remove_dir(path)?;
                    Err(e)
                }
                Err(e) => Err(e),
                Ok(v) => Ok(v),
            }
        } else {
            VectorWriterService::open(config)
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn new(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        let indexset = std::path::Path::new(&config.vectorset);
        if path.exists() {
            Err(node_error!("Shard does exist".to_string()))
        } else {
            let Some(similarity) = config.similarity.map(|i| i.into()) else {
                return Err(node_error!("A similarity must be specified, {:?}", config.similarity));
            };
            let index_metadata = IndexMetadata {
                similarity,
                channel: config.channel,
            };
            Ok(VectorWriterService {
                index: Writer::new(path, index_metadata)?,
                indexset: WriterSet::new(indexset)?,
                config: config.clone(),
            })
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn open(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        let indexset = std::path::Path::new(&config.vectorset);
        if !path.exists() {
            Err(node_error!("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Writer::open(path)?,
                indexset: WriterSet::new(indexset)?,
                config: config.clone(),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, UserVector, UserVectors, VectorSearchRequest,
        VectorSentence, VectorSimilarity,
    };
    use nucliadb_core::Channel;
    use std::collections::{HashMap, HashSet};
    use tempfile::TempDir;

    use super::*;
    use crate::service::reader::VectorReaderService;
    fn create_vector_set(writer: &mut VectorWriterService, set_name: String) -> (String, UserVectors) {
        let label = format!("{set_name}/label");
        let key = format!("{set_name}/key");
        let vector = vec![1.0, 3.0, 4.0];
        let data = UserVector {
            vector,
            labels: vec![label],
            ..Default::default()
        };
        let set = UserVectors {
            vectors: HashMap::from([(key, data)]),
        };
        let id = VectorSetId {
            shard: None,
            vectorset: set_name.clone(),
        };
        writer.add_vectorset(&id, VectorSimilarity::Cosine).unwrap();
        (set_name, set)
    }
    #[test]
    fn test_vectorset_functionality() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorsets"),
            channel: Channel::EXPERIMENTAL,
        };

        let mut writer = VectorWriterService::start(&vsc).expect("Error starting vector writer");
        let indexes: HashMap<_, _> = (0..10).map(|i| create_vector_set(&mut writer, i.to_string())).collect();
        let keys: HashSet<_> = indexes.keys().cloned().collect();
        let resource = Resource {
            vectors: indexes,
            ..Default::default()
        };
        writer.set_resource(&resource).unwrap();
        let index_keys: HashSet<_> = writer.list_vectorsets().unwrap().into_iter().collect();
        assert_eq!(index_keys, keys);

        let mut reader = VectorReaderService::start(&vsc).unwrap();
        let mut request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "4".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["4/label".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let context = VectorsContext::default();
        let results = reader.search(&request, &context).unwrap();
        let id = results.documents[0].doc_id.clone().unwrap().id;
        assert_eq!(results.documents.len(), 1);
        assert_eq!(id, "4/key");

        // Same set, but no label match
        request.field_labels = vec!["5/label".to_string()];
        let results = reader.search(&request, &context).unwrap();
        assert_eq!(results.documents.len(), 0);

        // Invalid set
        request.vector_set = "not a set".to_string();
        let results = reader.search(&request, &context).unwrap();
        assert_eq!(results.documents.len(), 0);

        // Remove set 4
        let id = VectorSetId {
            vectorset: "4".to_string(),
            ..Default::default()
        };
        let mut index_keys: HashSet<_> = writer.list_vectorsets().unwrap().into_iter().collect();
        writer.remove_vectorset(&id).unwrap();
        let index_keysp: HashSet<_> = writer.list_vectorsets().unwrap().into_iter().collect();
        index_keys.remove("4");
        assert!(!index_keysp.contains("4"));
        assert_eq!(index_keys, index_keysp);

        // Now vectorset 4 is no longer available
        request.vector_set = "4".to_string();
        request.field_labels = vec!["4/label".to_string()];

        reader.update().unwrap();
        let results = reader.search(&request, &context).unwrap();
        assert_eq!(results.documents.len(), 0);
    }

    #[test]
    fn test_new_vector_writer() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
            channel: Channel::EXPERIMENTAL,
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
            sentences,
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
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
    }

    #[test]
    fn test_get_segments() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
            channel: Channel::EXPERIMENTAL,
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
            sentences,
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
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());

        let segments = writer.get_segment_ids().unwrap();
        assert_eq!(segments.len(), 2);
        let existing_secs: Vec<String> = Vec::new();
        let Ok(IndexFiles::Other(index_files)) = writer.get_index_files(&existing_secs) else {
            panic!("Expected another outcome");
        };
        let mut expected_files = Vec::new();
        for segment in segments {
            expected_files.push(format!("vectors/{}/index.hnsw", segment));
            expected_files.push(format!("vectors/{}/journal.json", segment));
            expected_files.push(format!("vectors/{}/nodes.kv", segment));
        }
        assert_eq!(index_files.files, expected_files);
        assert_eq!(index_files.metadata_files.len(), 2);
    }
}
