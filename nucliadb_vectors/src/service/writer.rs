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

use crate::writer::Writer;

pub struct VectorWriterService {
    index: RwLock<Writer>,
}

impl Debug for VectorWriterService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorWriterService")
            .field("index", &self.index)
            .finish()
    }
}
impl WService for VectorWriterService {}
impl VectorServiceWriter for VectorWriterService {}
impl VectorWriterOnly for VectorWriterService {}
#[async_trait]
impl ServiceChild for VectorWriterService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping vector writer Service");
        self.index.write().unwrap().commit();
        Ok(())
    }
    fn count(&self) -> usize {
        self.index.read().unwrap().no_vectors()
    }
}

impl WriterChild for VectorWriterService {
    fn delete_resource(&mut self, resource_id: &ResourceId) -> InternalResult<()> {
        debug!("Delete resource in vector starts");
        self.index
            .write()
            .unwrap()
            .delete_document(resource_id.uuid.clone());
        self.index.write().unwrap().commit();
        debug!("Delete resource in vector ends");
        Ok(())
    }
    fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        debug!("Set resource in vector starts");
        if resource.status != ResourceStatus::Delete as i32 {
            let mut vector_id = 0;
            for paragraph in resource.paragraphs.values() {
                for index in paragraph.paragraphs.values() {
                    let mut labels = resource.labels.clone();
                    labels.append(&mut index.labels.clone());
                    for (key, sentence) in index.sentences.iter() {
                        vector_id += 1;
                        self.index.write().unwrap().insert(
                            key.clone(),
                            sentence.vector.clone(),
                            labels.clone(),
                        );
                        debug!("Vectors added {vector_id}");
                    }
                }
            }
            debug!("Commit on {vector_id}");
            self.index.write().unwrap().commit();
        }
        debug!("Set resource in vector ends");
        Ok(())
    }
    fn garbage_collection(&mut self) {
        self.index.write().unwrap().run_garbage_collection()
    }
}

impl VectorWriterService {
    pub async fn start(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(VectorWriterService::new(config).await.unwrap())
        } else {
            Ok(VectorWriterService::open(config).await.unwrap())
        }
    }
    pub async fn new(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(&path).await.unwrap();

            Ok(VectorWriterService {
                index: RwLock::new(Writer::new(&config.path)),
            })
        }
    }
    pub async fn open(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(VectorWriterService {
                index: RwLock::new(Writer::new(&config.path)),
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
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
        };
        let sentences: HashMap<String, VectorSentence> = vec![
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
        ]
        .iter()
        .map(|(v, k)| (v.clone(), VectorSentence { vector: k.clone() }))
        .collect();
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
            resource: None,
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
        let resource_id = ResourceId {
            shard_id: "DOC".to_string(),
            uuid: "DOC/KEY".to_string(),
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
