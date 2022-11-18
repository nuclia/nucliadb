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

use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{Resource, ResourceId, VectorSetId};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::vectors::data_point;
use crate::vectors::data_point_provider::*;
use crate::vectors::indexset::{IndexKeyCollector, IndexSet};

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
    fn list_vectorsets(&self) -> InternalResult<Vec<String>> {
        let mut collector = Vec::new();
        let indexset_slock = self.indexset.get_slock()?;
        self.indexset.index_keys(&mut collector, &indexset_slock);
        Ok(collector)
    }
    fn add_vectorset(&mut self, setid: &VectorSetId) -> InternalResult<()> {
        info!(
            "Adding vector index {} to {:?}",
            setid.vectorset, setid.shard
        );
        let indexid = setid.vectorset.as_str();
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset
            .get_or_create::<&str>(indexid, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;
        Ok(())
    }

    fn remove_vectorset(&mut self, setid: &VectorSetId) -> InternalResult<()> {
        info!(
            "Removing vector index {} from {:?}",
            setid.vectorset, setid.shard
        );
        let indexid = &setid.vectorset;
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset.remove_index(indexid, &indexset_elock)?;
        self.indexset.commit(indexset_elock)?;
        Ok(())
    }
}
impl WriterChild for VectorWriterService {
    fn stop(&mut self) -> InternalResult<()> {
        info!("Stopping vector writer Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let lock = self.index.get_slock().unwrap();
        self.index.no_nodes(&lock)
    }
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        info!("Delete resource in vector starts");
        let lock = self.index.get_elock()?;
        self.index.delete(&resource_id.uuid, &lock);
        self.index.commit(lock)?;
        info!("Delete resource in vector ends");
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        use data_point::{DataPoint, Elem, LabelDictionary};
        info!("Updating main index");
        info!("creating datapoints");
        let mut data_points = Vec::new();
        if resource.status != ResourceStatus::Delete as i32 {
            for paragraph in resource.paragraphs.values() {
                for (key, index) in paragraph.paragraphs.iter() {
                    let index_key = key.clone();
                    let labels = resource.labels.iter().chain(index.labels.iter()).cloned();
                    let labels = LabelDictionary::new(labels.collect());
                    let elems = index
                        .sentences
                        .iter()
                        .map(|(key, sentence)| (key.clone(), sentence.vector.clone()))
                        .map(|(key, sentence)| Elem::new(key, sentence, labels.clone()))
                        .collect::<Vec<_>>();
                    if !elems.is_empty() {
                        let new_dp = DataPoint::new(self.index.get_location(), elems)?;
                        data_points.push((index_key, new_dp));
                    }
                }
            }
        }
        info!("{} datapoints where created", data_points.len());
        let lock = self.index.get_elock()?;
        info!("Processing sentences to delete");
        for to_delete in &resource.sentences_to_delete {
            self.index.delete(to_delete, &lock)
        }
        info!("Indexing datapoints");
        for (key, data_point) in data_points {
            self.index.add(key, data_point, &lock);
        }
        self.index.commit(lock)?;

        info!("Updating vectorset indexes");
        // Updating existing indexes
        // Perform delete operations over the vector set
        info!("Processing delete requests for indexes in the set");
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
                index.delete(vector, &index_lock);
            });
            index.commit(index_lock)?;
        }
        std::mem::drop(indexset_slock);
        info!("Delete requests for indexes in the set processed");

        // Perform add operations over the vector set
        // New indexes may be created.
        info!("Creating and geting indexes in the set");
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
            let new_dp = DataPoint::new(index.get_location(), elems)?;
            let key = uuid::Uuid::new_v4().to_string();
            let lock = index.get_elock()?;
            index.add(key, new_dp, &lock);
            index.commit(lock)?;
        }
        info!("Create and update operations where applied to the indexes in the set");
        Ok(())
    }
    fn garbage_collection(&mut self) {}
}

impl VectorWriterService {
    pub fn start(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            VectorWriterService::new(config)
        } else {
            VectorWriterService::open(config)
        }
    }
    pub fn new(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        let indexset = std::path::Path::new(&config.vectorset);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Index::new(path, IndexCheck::None)?,
                indexset: IndexSet::new(indexset, IndexCheck::None)?,
            })
        }
    }
    pub fn open(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        let indexset = std::path::Path::new(&config.vectorset);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
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

    use nucliadb_protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, UserVector, UserVectors,
        VectorSearchRequest, VectorSentence,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::vectors::service::reader::VectorReaderService;

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
        let dir_vectorset = TempDir::new().unwrap();
        let vsc = VectorConfig {
            no_results: None,
            path: dir.path().to_str().unwrap().to_string(),
            vectorset: dir_vectorset.path().to_str().unwrap().to_string(),
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
            path: dir.path().to_str().unwrap().to_string(),
            vectorset: dir_vectorset.path().to_str().unwrap().to_string(),
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
            status: nucliadb_protos::resource::ResourceStatus::Processed as i32,
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
