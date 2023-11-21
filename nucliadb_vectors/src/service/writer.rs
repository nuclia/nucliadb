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
use std::collections::HashMap;
use std::fmt::Debug;
use std::fs;
use std::path::PathBuf;
use std::time::SystemTime;

use nucliadb_core::metrics::request_time;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::{metrics, Channel, IndexFiles};
use nucliadb_procs::measure;

use crate::data_point::{DataPoint, Elem, LabelDictionary};
use crate::data_point_provider::*;
use crate::indexset::{IndexKeyCollector, IndexSet};

impl IndexKeyCollector for Vec<String> {
    fn add_key(&mut self, key: String) {
        self.push(key);
    }
}

pub struct VectorWriterService {
    index: Index,
    indexset: IndexSet,
    channel: Channel,
    config: VectorConfig,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

impl VectorWriter for VectorWriterService {
    #[measure(actor = "vectors", metric = "list_vectorsets")]
    #[tracing::instrument(skip_all)]
    fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let time = SystemTime::now();

        let id: Option<String> = None;
        let mut collector = Vec::new();
        let indexset_slock = self.indexset.get_slock()?;
        self.indexset.index_keys(&mut collector, &indexset_slock);

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("{id:?} - Ending at {took} ms");

        Ok(collector)
    }

    #[measure(actor = "vectors", metric = "add_vectorset")]
    #[tracing::instrument(skip_all)]
    fn add_vectorset(
        &mut self,
        setid: &VectorSetId,
        similarity: VectorSimilarity,
    ) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = setid.shard.as_ref().map(|s| &s.id);
        let set = &setid.vectorset;
        let indexid = setid.vectorset.as_str();
        let similarity = similarity.into();
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset
            .get_or_create(indexid, similarity, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("{id:?}/{set} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "remove_vectorset")]
    #[tracing::instrument(skip_all)]
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = setid.shard.as_ref().map(|s| &s.id);
        let set = &setid.vectorset;
        let indexid = &setid.vectorset;
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset.remove_index(indexid, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("{id:?}/{set} - Ending at {took} ms");

        Ok(())
    }
}

impl WriterChild for VectorWriterService {
    #[measure(actor = "vectors", metric = "count")]
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let time = SystemTime::now();

        let id: Option<String> = None;
        let lock = self.index.get_slock()?;
        let no_nodes = self.index.no_nodes(&lock);
        std::mem::drop(lock);

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("{id:?} - Ending at {took} ms");

        Ok(no_nodes)
    }

    #[measure(actor = "vectors", metric = "delete_resource")]
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = Some(&resource_id.shard_id);
        let temporal_mark = TemporalMark::now();
        let lock = self.index.get_slock()?;
        self.index.delete(&resource_id.uuid, temporal_mark, &lock);
        self.index.commit(&lock)?;

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = resource.resource.as_ref().map(|i| &i.shard_id);
        debug!("{id:?} - Updating main index");
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating elements for the main index: starts {v} ms");
        }

        let temporal_mark = TemporalMark::now();
        let mut lengths: HashMap<usize, Vec<_>> = HashMap::new();
        let mut elems = Vec::new();
        if resource.status != ResourceStatus::Delete as i32 {
            for (paragraph_field, paragraph) in resource.paragraphs.iter() {
                let field = &[paragraph_field.clone()];
                for index in paragraph.paragraphs.values() {
                    let labels = LabelDictionary::new(
                        resource
                            .labels
                            .iter()
                            .chain(index.labels.iter())
                            .chain(field.iter())
                            .cloned()
                            .collect(),
                    );
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
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating elements for the main index: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Datapoint creation: starts {v} ms");
        }

        if lengths.len() > 1 {
            return Ok(tracing::error!("{}", self.dimensions_report(lengths)));
        }

        let new_dp = if !elems.is_empty() {
            Some(DataPoint::new(
                self.index.location(),
                elems,
                Some(temporal_mark),
                self.index.metadata().similarity,
                self.channel,
            )?)
        } else {
            None
        };

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Datapoint creation: ends {v} ms");
        }

        let lock = self.index.get_slock()?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Processing Sentences to delete: starts {v} ms");
        }
        for to_delete in &resource.sentences_to_delete {
            self.index.delete(to_delete, temporal_mark, &lock)
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Processing Sentences to delete: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Indexing datapoint: starts {v} ms");
        }
        match new_dp.map(|i| self.index.add(i, &lock)).unwrap_or(Ok(())) {
            Ok(_) => self.index.commit(&lock)?,
            Err(e) => tracing::error!("{id:?}/default could insert vectors: {e:?}"),
        }
        std::mem::drop(lock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Indexing datapoint: ends {v} ms");
        }

        // Updating existing indexes
        // Perform delete operations over the vector set
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Delete requests for indexes in the set: starts {v} ms");
        }
        let indexset_slock = self.indexset.get_slock()?;
        let index_iter = resource.vectors_to_delete.iter().flat_map(|(k, v)| {
            self.indexset
                .get(k, &indexset_slock)
                .transpose()
                .map(|i| (v, i))
        });
        for (vectorlist, index) in index_iter {
            let mut index = index?;
            let index_lock = index.get_slock()?;
            vectorlist.vectors.iter().for_each(|vector| {
                index.delete(vector, temporal_mark, &index_lock);
            });
            index.commit(&index_lock)?;
        }
        std::mem::drop(indexset_slock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Delete requests for indexes in the set: ends {v} ms");
        }

        // Perform add operations over the vector set
        // New indexes may be created.
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating and geting indexes in the set: starts {v} ms");
        }
        let indexset_elock = self.indexset.get_elock()?;
        let indexes = resource
            .vectors
            .keys()
            .map(|k| (k, self.indexset.get(k, &indexset_elock)))
            .map(|(key, index)| index.map(|index| (key, index)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        self.indexset.commit(indexset_elock)?;

        // Inner indexes are updated
        for (index_key, mut index) in indexes.into_iter().flat_map(|i| i.1.map(|j| (i.0, j))) {
            let mut lengths: HashMap<usize, Vec<_>> = HashMap::new();
            let mut elems = vec![];
            for (vectorset, user_vector) in resource.vectors[index_key].vectors.iter() {
                let key = vectorset.clone();
                let vector = user_vector.vector.clone();
                let bucket = lengths.entry(vector.len()).or_default();
                let labels = LabelDictionary::new(user_vector.labels.clone());
                elems.push(Elem::new(key, vector, labels, None));
                bucket.push(vectorset);
            }
            if lengths.len() > 1 {
                tracing::error!("vectorsets report: {}", self.dimensions_report(lengths));
            } else if !elems.is_empty() {
                let similarity = index.metadata().similarity;
                let location = index.location();
                let new_dp = DataPoint::new(
                    location,
                    elems,
                    Some(temporal_mark),
                    similarity,
                    self.channel,
                )?;
                let lock = index.get_slock()?;
                match index.add(new_dp, &lock) {
                    Ok(_) => index.commit(&lock)?,
                    Err(e) => tracing::error!("Could not insert at {id:?}/{index_key}: {e:?}"),
                }
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating and geting indexes in the set: ends {v} ms");
        }

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("set_resource".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }

    #[measure(actor = "vectors", metric = "garbage_collection")]
    #[tracing::instrument(skip_all)]
    fn garbage_collection(&mut self) -> NodeResult<()> {
        let time = SystemTime::now();

        let lock = self.index.try_elock()?;
        self.index.collect_garbage(&lock)?;

        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        debug!("Garbage collection {took} ms");

        Ok(())
    }

    fn get_segment_ids(&self) -> NodeResult<Vec<String>> {
        let mut seg_ids = self.get_segment_ids_for_vectorset(&self.index.location)?;
        let vectorsets = self.list_vectorsets()?;
        for vs in vectorsets {
            let vs_seg_ids = self.get_segment_ids_for_vectorset(&self.config.vectorset.join(vs))?;
            seg_ids.extend(vs_seg_ids);
        }
        Ok(seg_ids)
    }

    fn get_index_files(&self, ignored_segment_ids: &[String]) -> NodeResult<IndexFiles> {
        // Should be called along with a lock at a higher level to be safe
        let mut meta_files = HashMap::new();
        meta_files.insert(
            "vectors/state.bincode".to_string(),
            fs::read(self.config.path.join("state.bincode"))?,
        );
        meta_files.insert(
            "vectors/metadata.json".to_string(),
            fs::read(self.config.path.join("metadata.json"))?,
        );

        let mut files = Vec::new();

        for segment_id in self.get_segment_ids_for_vectorset(&self.index.location)? {
            if ignored_segment_ids.contains(&segment_id) {
                continue;
            }
            files.push(format!("vectors/{}/index.hnsw", segment_id));
            files.push(format!("vectors/{}/journal.json", segment_id));
            files.push(format!("vectors/{}/nodes.kv", segment_id));

            let fst_path = self.index.location.join(format!("{}/fst", segment_id));
            if fst_path.exists() {
                files.push(format!("vectors/{}/fst/keys.fst", segment_id));
                files.push(format!("vectors/{}/fst/labels.fst", segment_id));
                files.push(format!("vectors/{}/fst/labels.idx", segment_id));
            }
        }

        let vectorsets = self.list_vectorsets()?;
        if !vectorsets.is_empty() {
            meta_files.insert(
                "vectorset/state.bincode".to_string(),
                fs::read(self.config.vectorset.join("state.bincode"))?,
            );
            for vs in vectorsets {
                for segment_id in
                    self.get_segment_ids_for_vectorset(&self.config.vectorset.join(vs.clone()))?
                {
                    if ignored_segment_ids.contains(&segment_id) {
                        continue;
                    }
                    files.push(format!("vectorset/{}/{}/index.hnsw", vs, segment_id));
                    files.push(format!("vectorset/{}/{}/journal.json", vs, segment_id));
                    files.push(format!("vectorset/{}/{}/nodes.kv", vs, segment_id));

                    let fst_path = self.config.vectorset.join(format!("{}/fst", segment_id));
                    if fst_path.exists() {
                        files.push(format!("vectors/{}/fst/keys.fst", segment_id));
                        files.push(format!("vectors/{}/fst/labels.fst", segment_id));
                        files.push(format!("vectors/{}/fst/labels.idx", segment_id));
                    }
                }
                meta_files.insert(
                    format!("vectorset/{}/state.bincode", vs),
                    fs::read(self.config.vectorset.join(format!("{}/state.bincode", vs)))?,
                );
                meta_files.insert(
                    format!("vectorset/{}/metadata.json", vs),
                    fs::read(self.config.vectorset.join(format!("{}/metadata.json", vs)))?,
                );
            }
        }

        if files.is_empty() {
            // exit with no changes
            return Ok(IndexFiles {
                metadata_files: HashMap::new(),
                files,
            });
        }

        Ok(IndexFiles {
            metadata_files: meta_files,
            files,
        })
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
                return Err(node_error!("A similarity must be specified"));
            };
            Ok(VectorWriterService {
                index: Index::new(
                    path,
                    IndexMetadata {
                        similarity,
                        channel: config.channel,
                    },
                )?,
                indexset: IndexSet::new(indexset)?,
                channel: config.channel,
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
                index: Index::open(path)?,
                indexset: IndexSet::new(indexset)?,
                channel: config.channel,
                config: config.clone(),
            })
        }
    }

    fn get_segment_ids_for_vectorset(&self, location: &PathBuf) -> NodeResult<Vec<String>> {
        let mut ids = Vec::new();
        for dir_entry in std::fs::read_dir(location)? {
            let entry = dir_entry?;
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().to_string();
            if path.is_file() {
                continue;
            }
            ids.push(name);
        }
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, UserVector, UserVectors,
        VectorSearchRequest, VectorSentence, VectorSimilarity,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::service::reader::VectorReaderService;
    fn create_vector_set(
        writer: &mut VectorWriterService,
        set_name: String,
    ) -> (String, UserVectors) {
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
        assert_eq!(writer.channel, Channel::EXPERIMENTAL);

        let indexes: HashMap<_, _> = (0..10)
            .map(|i| create_vector_set(&mut writer, i.to_string()))
            .collect();
        let keys: HashSet<_> = indexes.keys().cloned().collect();
        let resource = Resource {
            vectors: indexes,
            ..Default::default()
        };
        writer.set_resource(&resource).unwrap();
        let index_keys: HashSet<_> = writer.list_vectorsets().unwrap().into_iter().collect();
        assert_eq!(index_keys, keys);

        let reader = VectorReaderService::start(&vsc).unwrap();
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
        let results = reader.search(&request).unwrap();
        let id = results.documents[0].doc_id.clone().unwrap().id;
        assert_eq!(results.documents.len(), 1);
        assert_eq!(id, "4/key");

        // Same set, but no label match
        request.field_labels = vec!["5/label".to_string()];
        let results = reader.search(&request).unwrap();
        assert_eq!(results.documents.len(), 0);

        // Invalid set
        request.vector_set = "not a set".to_string();
        let results = reader.search(&request).unwrap();
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
        let results = reader.search(&request).unwrap();
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
        let existing_segs: Vec<String> = Vec::new();
        let index_files = writer.get_index_files(&existing_segs).unwrap();
        let mut expected_files = Vec::new();
        for segment in segments {
            expected_files.push(format!("vectors/{}/index.hnsw", segment));
            expected_files.push(format!("vectors/{}/journal.json", segment));
            expected_files.push(format!("vectors/{}/nodes.kv", segment));
            expected_files.push(format!("vectors/{}/fst/keys.fst", segment));
            expected_files.push(format!("vectors/{}/fst/labels.fst", segment));
            expected_files.push(format!("vectors/{}/fst/labels.idx", segment));
        }
        assert_eq!(index_files.files, expected_files);
        assert_eq!(index_files.metadata_files.len(), 2);
    }
}
