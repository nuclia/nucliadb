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
use nucliadb_protos::{
    DocumentScored, DocumentVectorIdentifier, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::reader::Reader;

pub struct VectorReaderService {
    no_results: usize,
    index: RwLock<Reader>,
}
impl Debug for VectorReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReaderService")
            .field("index", &self.index)
            .finish()
    }
}

impl RService for VectorReaderService {}
impl VectorServiceReader for VectorReaderService {}
impl VectorReaderOnly for VectorReaderService {}
#[async_trait]
impl ServiceChild for VectorReaderService {
    async fn stop(&self) -> InternalResult<()> {
        info!("Stopping vector reader Service");
        Ok(())
    }
    fn count(&self) -> usize {
        self.index.read().unwrap().no_vectors()
    }
}

impl ReaderChild for VectorReaderService {
    type Request = VectorSearchRequest;
    type Response = VectorSearchResponse;
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        debug!(
            "{} {} {}",
            self.no_results,
            request.tags.len(),
            request.vector.len()
        );
        let raw_result = self.index.read().unwrap().search(
            request.vector.clone(),
            request.tags.clone(),
            self.no_results,
        );
        let mut documents = Vec::with_capacity(raw_result.len());
        for (id, distance) in raw_result {
            documents.push(DocumentScored {
                doc_id: Some(DocumentVectorIdentifier { id }),
                score: distance,
            });
        }
        Ok(VectorSearchResponse { documents })
    }

    fn reload(&self) {
        self.index.write().unwrap().reload();
    }
    fn stored_ids(&self) -> Vec<String> {
        self.index.read().unwrap().keys()
    }
}

impl VectorReaderService {
    pub async fn start(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Ok(VectorReaderService::new(config).await.unwrap())
        } else {
            Ok(VectorReaderService::open(config).await.unwrap())
        }
    }
    pub async fn new(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            tokio::fs::create_dir_all(&path).await.unwrap();

            Ok(VectorReaderService {
                no_results: config.no_results.unwrap(),
                index: RwLock::new(Reader::new(&config.path)),
            })
        }
    }

    pub async fn open(config: &VectorServiceConfiguration) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(VectorReaderService {
                no_results: config.no_results.unwrap(),
                index: RwLock::new(Reader::new(&config.path)),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nucliadb_protos::{IndexParagraph, IndexParagraphs, Resource, VectorSentence};
    use tempdir::TempDir;

    use super::*;
    use crate::service::writer::VectorWriterService;

    #[tokio::test]
    async fn test_new_vector_reader() {
        let dir = TempDir::new("payload_dir").unwrap();
        let vsc = VectorServiceConfiguration {
            no_results: Some(3),
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
            labels: vec!["1".to_string()],
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
            labels: vec!["2".to_string()],
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
        writer.stop().await.unwrap();
        let reader = VectorReaderService::start(&vsc).await.unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            tags: vec!["1".to_string()],
            reload: false,
        };
        let result = reader.search(&request).unwrap();
        assert_eq!(result.documents.len(), reader.no_results);
    }
}
