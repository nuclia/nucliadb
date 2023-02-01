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
use std::time::SystemTime;

use data_point::{DataPoint, Elem, LabelDictionary};
use nucliadb_core::prelude::*;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId, VectorSetId};
use tracing::*;

use crate::data_point;
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
        let mut collector = Vec::new();
        let indexset_slock = self.indexset.get_slock()?;
        self.indexset.index_keys(&mut collector, &indexset_slock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(collector)
    }
    #[tracing::instrument(skip_all)]
    fn add_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()> {
        let id = setid.shard.as_ref().map(|s| &s.id);
        let time = SystemTime::now();
        let set = &setid.vectorset;
        let indexid = setid.vectorset.as_str();
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset.get_or_create(indexid, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?}/{set} - Ending at {v} ms");
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn remove_vectorset(&mut self, setid: &VectorSetId) -> NodeResult<()> {
        let id = setid.shard.as_ref().map(|s| &s.id);
        let time = SystemTime::now();
        let set = &setid.vectorset;
        let indexid = &setid.vectorset;
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset.remove_index(indexid, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?}/{set} - Ending at {v} ms");
        }
        Ok(())
    }
}
impl WriterChild for VectorWriterService {
    #[tracing::instrument(skip_all)]
    fn stop(&mut self) -> NodeResult<()> {
        info!("Stopping vector writer Service");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn count(&self) -> usize {
        let id: Option<String> = None;
        let time = SystemTime::now();
        let lock = self.index.get_slock().unwrap();
        let no_nodes = self.index.no_nodes(&lock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms");
        }
        no_nodes
    }
    #[tracing::instrument(skip_all)]
    fn delete_resource(&mut self, resource_id: &ResourceId) -> NodeResult<()> {
        let id = Some(&resource_id.shard_id);
        let time = SystemTime::now();

        let temporal_mark = TemporalMark::now();
        let lock = self.index.get_elock()?;
        self.index.delete(&resource_id.uuid, temporal_mark, &lock);
        self.index.commit(lock)?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let id = resource.resource.as_ref().map(|i| &i.shard_id);
        let time = SystemTime::now();
        info!("{id:?} - Updating main index");
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating elements for the main index: starts {v} ms");
        }

        let temporal_mark = TemporalMark::now();
        let mut elems = Vec::new();
        if resource.status != ResourceStatus::Delete as i32 {
            for paragraph in resource.paragraphs.values() {
                for index in paragraph.paragraphs.values() {
                    let labels = resource.labels.iter().chain(index.labels.iter()).cloned();
                    let labels = LabelDictionary::new(labels.collect());
                    index
                        .sentences
                        .iter()
                        .map(|(key, sentence)| (key.clone(), sentence.vector.clone()))
                        .map(|(key, sentence)| Elem::new(key, sentence, labels.clone()))
                        .for_each(|e| elems.push(e));
                }
            }
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating elements for the main index: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Datapoint creation: starts {v} ms");
        }
        let new_dp = DataPoint::new(self.index.get_location(), elems, Some(temporal_mark))?;
        let no_nodes = new_dp.meta().no_nodes();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Datapoint creation: ends {v} ms");
        }

        let lock = self.index.get_elock()?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Processing Sentences to delete: starts {v} ms");
        }
        for to_delete in &resource.sentences_to_delete {
            self.index.delete(to_delete, temporal_mark, &lock)
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Processing Sentences to delete: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Indexing datapoint: starts {v} ms");
        }
        if no_nodes == 0 {
            info!("{id:?} - The datapoint is empty, no need to add it");
            self.index.commit(lock)?;
        } else {
            info!("{id:?} - The datapoint is not empty, adding it");
            self.index.add(new_dp, &lock);
            self.index.commit(lock)?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Indexing datapoint: ends {v} ms");
        }

        // Updating existing indexes
        // Perform delete operations over the vector set
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Delete requests for indexes in the set: starts {v} ms");
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
            let index_lock = index.get_elock()?;
            vectorlist.vectors.iter().for_each(|vector| {
                index.delete(vector, temporal_mark, &index_lock);
            });
            index.commit(index_lock)?;
        }
        std::mem::drop(indexset_slock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Delete requests for indexes in the set: ends {v} ms");
        }

        // Perform add operations over the vector set
        // New indexes may be created.
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating and geting indexes in the set: starts {v} ms");
        }
        let indexset_elock = self.indexset.get_elock()?;
        let indexes = resource
            .vectors
            .keys()
            .map(|k| (k, self.indexset.get_or_create::<&str>(k, &indexset_elock)))
            .map(|(key, index)| index.map(|index| (key, index)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        self.indexset.commit(indexset_elock)?;

        // Inner indexes are updated
        for (index_key, mut index) in indexes {
            let mut elems = vec![];
            for (key, user_vector) in resource.vectors[index_key].vectors.iter() {
                let key = key.clone();
                let vector = user_vector.vector.clone();
                let labels = LabelDictionary::new(user_vector.labels.clone());
                elems.push(Elem::new(key, vector, labels));
            }
            let new_dp = DataPoint::new(index.get_location(), elems, Some(temporal_mark))?;
            let lock = index.get_elock()?;
            index.add(new_dp, &lock);
            index.commit(lock)?;
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating and geting indexes in the set: ends {v} ms");
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn garbage_collection(&mut self) {}
}

impl VectorWriterService {
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
            Ok(VectorWriterService {
                index: Index::new(path, IndexCheck::None)?,
                indexset: IndexSet::new(indexset, IndexCheck::None)?,
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
                index: Index::new(path, IndexCheck::Sanity)?,
                indexset: IndexSet::new(indexset, IndexCheck::Sanity)?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, UserVector, UserVectors,
        VectorSearchRequest, VectorSentence,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::service::reader::VectorReaderService;

    fn create_vector_set(set_name: String) -> (String, UserVectors) {
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
        (set_name, set)
    }
    #[test]
    fn test_vectorset_functionality() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            no_results: None,
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorsets"),
        };
        let indexes: HashMap<_, _> = (0..10).map(|i| create_vector_set(i.to_string())).collect();
        let keys: HashSet<_> = indexes.keys().cloned().collect();
        let mut writer = VectorWriterService::start(&vsc).unwrap();
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
            reload: false,
            with_duplicates: false,
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
        request.tags = vec!["4/label".to_string()];
        let results = reader.search(&request).unwrap();
        assert_eq!(results.documents.len(), 0);
    }

    #[test]
    fn test_new_vector_writer() {
        let dir = TempDir::new().unwrap();
        let dir_vectorset = TempDir::new().unwrap();
        let vsc = VectorConfig {
            no_results: None,
            path: dir.path().to_path_buf(),
            vectorset: dir_vectorset.path().to_path_buf(),
        };
        let sentences: HashMap<String, VectorSentence> = vec![
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
        ]
        .iter()
        .map(|(v, k)| (v.clone(), VectorSentence { vector: k.clone() }))
        .collect();
        let resource_id = ResourceId {
            shard_id: "DOC".to_string(),
            uuid: "DOC/KEY".to_string(),
        };
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
        writer.stop().unwrap();
    }
}
