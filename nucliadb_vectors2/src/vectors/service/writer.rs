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

use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::vectors::data_point;
use crate::vectors::data_point_provider::*;

pub struct VectorWriterService {
    index: Index,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}

impl VectorWriter for VectorWriterService {}
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
        info!("Set resource in vector starts");
        let resource_id = resource.resource.as_ref().unwrap().uuid.clone();

        info!("Indexing resource contents");
        let mut elems = vec![];
        if resource.status != ResourceStatus::Delete as i32 {
            for paragraph in resource.paragraphs.values() {
                for index in paragraph.paragraphs.values() {
                    let mut labels = resource.labels.clone();
                    labels.append(&mut index.labels.clone());
                    let labels = LabelDictionary::new(labels);
                    index
                        .sentences
                        .iter()
                        .map(|(key, sentence)| (key.clone(), sentence.vector.clone()))
                        .map(|(key, sentence)| Elem::new(key, sentence, labels.clone()))
                        .for_each(|elem| elems.push(elem));
                }
            }
        }
        info!("Resource contents indexed");

        info!("Adding entry for new index");
        let lock = self.index.get_elock()?;
        for to_delete in &resource.sentences_to_delete {
            self.index.delete(to_delete, &lock)
        }
        if !elems.is_empty() {
            let new_dp = DataPoint::new(self.index.get_location(), elems)?;
            self.index.add(resource_id, new_dp, &lock);
        }
        self.index.commit(lock)?;
        info!("Entry for new index added");

        info!("Set resource in vector ends");
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
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            std::fs::create_dir_all(path).unwrap();
            Ok(VectorWriterService {
                index: Index::writer(path)?,
            })
        }
    }
    pub fn open(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: Index::writer(path)?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nucliadb_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence};
    use tempdir::TempDir;

    use super::*;

    #[test]
    fn test_new_vector_writer() {
        let dir = TempDir::new("payload_dir").unwrap();
        let vsc = VectorConfig {
            no_results: None,
            path: dir.path().to_str().unwrap().to_string(),
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
