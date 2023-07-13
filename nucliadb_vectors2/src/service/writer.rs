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
use std::fmt::Debug;
use std::fs::File;
use std::path::PathBuf;
use std::time::SystemTime;

use fs2::FileExt;
use nucliadb_core::metrics;
use nucliadb_core::metrics::request_time;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::tracing::{self, *};

use super::SET_LOCK;
use crate::data_point::{DataPoint, Elem, LabelDictionary};
use crate::data_point_provider::*;
pub struct VectorWriterService {
    rest_lock: File,
    rest: PathBuf,
    default: Writer,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

impl VectorWriter for VectorWriterService {
    #[tracing::instrument(skip_all)]
    fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let id: Option<String> = None;
        let time = SystemTime::now();

        let sets: Vec<_> = std::fs::read_dir(&self.rest)?
            .flatten()
            .map(|entry| entry.path())
            .filter(|entry| entry.is_dir())
            .flat_map(|entry| entry.file_name().map(|i| i.to_os_string()))
            .map(|i| i.to_string_lossy().to_string())
            .collect();

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("list_vectorsets".to_string());
        metrics.record_request_time(metric, took);

        debug!("{id:?} - Ending at {took} ms");
        Ok(sets)
    }

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
        let metadata = IndexMetadata { similarity };
        let index_location = self.rest.join(indexid);
        std::fs::create_dir(&index_location)?;
        Index::new(&index_location, metadata)?;

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("add_vectorset".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?}/{set} - Ending at {took} ms");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = setid.shard.as_ref().map(|s| &s.id);
        let set = &setid.vectorset;
        let indexid = &setid.vectorset;
        let index_location = self.rest.join(indexid);
        self.rest_lock.lock_exclusive()?;
        std::fs::remove_dir_all(index_location)?;
        self.rest_lock.unlock()?;

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("remove_vectorset".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?}/{set} - Ending at {took} ms");

        Ok(())
    }
}
impl WriterChild for VectorWriterService {
    #[tracing::instrument(skip_all)]
    fn count(&self) -> NodeResult<usize> {
        let time = SystemTime::now();
        let id: Option<String> = None;

        let no_nodes = self.default.number_of_nodes();

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("count".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?} - Ending at {took} ms");

        Ok(no_nodes)
    }
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = Some(&resource_id.shard_id);
        let temporal_mark = TemporalMark::now();
        self.default.delete(resource_id.uuid.clone(), temporal_mark);
        self.default.commit()?;

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("delete_resource".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let time = SystemTime::now();

        let id = resource.resource.as_ref().map(|i| &i.shard_id);
        debug!("{id:?} - Updating main index");
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Creating elements for the main index: starts {v} ms");
        }

        let temporal_mark = TemporalMark::now();
        let mut elems = Vec::new();
        if resource.status != ResourceStatus::Delete as i32 {
            for (field, paragraph) in resource.paragraphs.iter() {
                let field = &[field.clone()];
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
                        elems.push(Elem::new(key, vector, labels, metadata));
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

        if !elems.is_empty() {
            let location = self.default.location();
            let similarity = self.default.metadata().similarity;
            let dp = DataPoint::new(location, elems, Some(temporal_mark), similarity)?;
            self.default.add(dp);
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Datapoint creation: ends {v} ms");
        }

        for to_delete in resource.sentences_to_delete.iter().cloned() {
            self.default.delete(to_delete, temporal_mark);
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - default index commit: starts {v} ms");
        }
        self.default.commit()?;

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - default index commit: ends {v} ms");
        }

        // Updating existing indexes
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Modifying sets: starts {v} ms");
        }

        let deletes = resource.vectors_to_delete.keys();
        let adds = resource.vectors.keys();
        for index_name in deletes.chain(adds) {
            let index_path = self.rest.join(index_name);
            let mut writer = Index::open(&index_path).and_then(|i| Index::writer(&i))?;
            if let Some(vectors_to_delete) = resource.vectors_to_delete.get(index_name) {
                for key in vectors_to_delete.vectors.iter().cloned() {
                    writer.delete(key, temporal_mark)
                }
            }
            if let Some(vectors_to_insert) = resource.vectors.get(index_name) {
                let mut elems = vec![];
                for (key, user_vector) in vectors_to_insert.vectors.iter() {
                    let key = key.clone();
                    let vector = user_vector.vector.clone();
                    let labels = LabelDictionary::new(user_vector.labels.clone());
                    elems.push(Elem::new(key, vector, labels, None));
                }
                if !elems.is_empty() {
                    let location = writer.location();
                    let similarity = writer.metadata().similarity;
                    let dp = DataPoint::new(location, elems, Some(temporal_mark), similarity)?;
                    writer.add(dp);
                }
            }
            writer.commit()?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            debug!("{id:?} - Modifying set: ends {v} ms");
        }

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("set_resource".to_string());
        metrics.record_request_time(metric, took);
        debug!("{id:?} - Ending at {took} ms");

        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn garbage_collection(&mut self) -> NodeResult<()> {
        let time = SystemTime::now();

        self.default.collect_garbage()?;

        let metrics = metrics::get_metrics();
        let took = time.elapsed().map(|i| i.as_secs_f64()).unwrap_or(f64::NAN);
        let metric = request_time::RequestTimeKey::vectors("garbage_collection".to_string());
        metrics.record_request_time(metric, took);
        debug!("Garbage collection {took} ms");

        Ok(())
    }
}

impl VectorWriterService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &VectorConfig) -> NodeResult<Self> {
        if !config.path.exists() {
            VectorWriterService::new(config)
        } else {
            VectorWriterService::open(config)
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn new(config: &VectorConfig) -> NodeResult<Self> {
        std::fs::create_dir(&config.path)?;
        std::fs::create_dir(&config.vectorset)?;
        let Some(similarity) = config.similarity.map(|i| i.into()) else {
            return Err(node_error!("A similarity must be specified"));
        };
        let metadata = IndexMetadata { similarity };
        let default = Index::new(&config.path, metadata).and_then(|i| Index::writer(&i))?;
        let rest = config.vectorset.clone();
        let rest_lock = File::create(rest.join(SET_LOCK))?;
        Ok(VectorWriterService {
            default,
            rest,
            rest_lock,
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn open(config: &VectorConfig) -> NodeResult<Self> {
        let rest = config.vectorset.clone();
        let rest_path = rest.join(SET_LOCK);
        let default = Index::open(&config.path).and_then(|i| Index::writer(&i))?;
        let rest_lock = File::open(&rest_path).or_else(|_| File::create(&rest_path))?;
        Ok(VectorWriterService {
            default,
            rest,
            rest_lock,
        })
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
        };

        let mut writer = VectorWriterService::start(&vsc).unwrap();
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
            tags: vec!["4/label".to_string()],
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
        request.tags = vec!["5/label".to_string()];
        let results = reader.search(&request).unwrap();
        assert_eq!(results.documents.len(), 0);

        // Invalid set
        request.vector_set = "not a set".to_string();
        let result = reader.search(&request);
        assert!(result.is_err());

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
        request.tags = vec!["4/label".to_string()];
        let result = reader.search(&request);
        assert!(result.is_err());
    }

    #[test]
    fn test_new_vector_writer() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
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
            relations_to_delete: vec![],
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
}
