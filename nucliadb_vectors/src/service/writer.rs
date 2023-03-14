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

use data_point::{Elem, LabelDictionary};
use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::resource::ResourceStatus;
use nucliadb_core::protos::{Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::tracing::{self, *};

use crate::data_point::DataPoint;
use crate::data_point_provider::*;
use crate::indexset::{IndexKeyCollector, IndexSet};
use crate::{data_point, VectorErr};

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
    fn add_vectorset(
        &mut self,
        setid: &VectorSetId,
        similarity: VectorSimilarity,
    ) -> NodeResult<()> {
        let id = setid.shard.as_ref().map(|s| &s.id);
        let time = SystemTime::now();
        let set = &setid.vectorset;
        let indexid = setid.vectorset.as_str();
        let similarity = similarity.into();
        let indexset_elock = self.indexset.get_elock()?;
        self.indexset
            .get_or_create(indexid, similarity, &indexset_elock)?;
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
    fn count(&self) -> NodeResult<usize> {
        let id: Option<String> = None;
        let time = SystemTime::now();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Count starting at {v} ms");
        }
        let lock = self.index.get_slock()?;
        let no_nodes = self.index.no_nodes(&lock);
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms");
        }
        Ok(no_nodes)
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
            info!("{id:?} - Creating elements for the main index: ends {v} ms");
        }

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Datapoint creation: starts {v} ms");
        }

        let similarity = self.index.metadata().similarity;
        let new_dp = if !elems.is_empty() {
            Some(DataPoint::new(
                self.index.location(),
                elems,
                Some(temporal_mark),
                similarity,
            )?)
        } else {
            None
        };

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
        if let Some(new_dp) = new_dp {
            info!("{id:?} - The datapoint is not empty, adding it");
            self.index.add(new_dp, &lock);
            self.index.commit(lock)?;
        } else {
            info!("{id:?} - The datapoint is empty, no need to add it");
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
            let index = index?;
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
            .map(|k| (k, self.indexset.get(k, &indexset_elock)))
            .map(|(key, index)| index.map(|index| (key, index)))
            .collect::<Result<HashMap<_, _>, _>>()?;
        self.indexset.commit(indexset_elock)?;

        // Inner indexes are updated
        for (index_key, index) in indexes.into_iter().flat_map(|i| i.1.map(|j| (i.0, j))) {
            let mut elems = vec![];
            for (key, user_vector) in resource.vectors[index_key].vectors.iter() {
                let key = key.clone();
                let vector = user_vector.vector.clone();
                let labels = LabelDictionary::new(user_vector.labels.clone());
                elems.push(Elem::new(key, vector, labels, None));
            }
            if !elems.is_empty() {
                let similarity = index.metadata().similarity;
                let new_dp =
                    DataPoint::new(index.location(), elems, Some(temporal_mark), similarity)?;
                let lock = index.get_elock()?;
                index.add(new_dp, &lock);
                index.commit(lock)?;
            }
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
    fn garbage_collection(&mut self) -> NodeResult<()> {
        self.collect_garbage_for(&self.index)?;
        let indexset_slock = self.indexset.get_slock()?;
        let mut index_keys = vec![];
        self.indexset.index_keys(&mut index_keys, &indexset_slock);
        for index_key in index_keys {
            let Some(index) = self.indexset.get(&index_key, &indexset_slock)? else {
                return Err(node_error!("Unknown state for {index_key}"));
            };
            self.collect_garbage_for(&index)?;
        }
        Ok(())
    }
}

impl VectorWriterService {
    fn collect_garbage_for(&self, index: &Index) -> NodeResult<()> {
        info!("Collecting garbage for index: {:?}", index.location());
        let slock = index.get_slock()?;
        match index.collect_garbage(&slock) {
            Ok(_) => info!("Garbage collected for main index"),
            Err(VectorErr::WorkDelayed) => info!("Garbage collection delayed"),
            Err(e) => Err(e)?,
        }
        Ok(())
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
                index: Index::new(path, IndexMetadata { similarity })?,
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
                index: Index::open(path, IndexCheck::Sanity)?,
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
            no_results: None,
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
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            no_results: None,
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
        writer.stop().unwrap();
    }
}
