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
use std::sync::RwLock;

use async_trait::async_trait;
use nucliadb_protos::resource::ResourceStatus;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::data_point;
use crate::data_point_provider::*;

pub struct VectorWriterService {
    index: RwLock<Index>,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService").finish()
    }
}
impl WService for VectorWriterService {}
#[async_trait]
impl ServiceChild for VectorWriterService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping vector writer Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let index = self.index.read().unwrap();
        let lock = index.get_slock().unwrap();
        index.no_nodes(&lock)
    }
}

impl WriterChild for VectorWriterService {
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        info!("Delete resource in vector starts");
        let mut index = self.index.write().unwrap();
        let lock = index.get_elock()?;
        index.delete(&resource_id.uuid, &lock);
        index.commit(lock)?;
        info!("Delete resource in vector ends");
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        use data_point::{DataPoint, Elem, LabelDictionary};
        info!("Set resource in vector starts");
        let resource_id = resource.resource.as_ref().unwrap().uuid.clone();
        let mut elems = vec![];
        let mut provider = self.index.write().unwrap();
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
        if !elems.is_empty() {
            let new_dp = DataPoint::new(provider.get_location(), elems)?;
            let lock = provider.get_elock()?;
            provider.add(resource_id, new_dp, &lock);
            provider.commit(lock)?;
        }
        info!("Set resource in vector ends");
        Ok(())
    }
    fn garbage_collection(&mut self) {}
}

impl VectorWriterService {
    pub async fn start(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            VectorWriterService::new(config).await
        } else {
            VectorWriterService::open(config).await
        }
    }
    pub async fn new(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(&path).await.unwrap();
            Ok(VectorWriterService {
                index: RwLock::new(Index::new(path)?),
            })
        }
    }
    pub async fn open(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: RwLock::new(Index::new(path)?),
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

    #[tokio::test]
    async fn test_new_vector_writer() {
        let dir = TempDir::new("payload_dir").unwrap();
        let vsc = VectorServiceConfiguration {
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
        let mut writer = VectorWriterService::start(&vsc).await.unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        let res = writer.delete_resource(&resource_id);
        assert!(res.is_ok());
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        writer.stop().await.unwrap();
    }
}
