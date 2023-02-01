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
use std::time::SystemTime;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::{
    DocumentScored, DocumentVectorIdentifier, VectorSearchRequest, VectorSearchResponse,
};
use tracing::*;

use crate::data_point_provider::*;
use crate::indexset::IndexSet;

impl<'a> SearchRequest for (usize, &'a VectorSearchRequest) {
    fn with_duplicates(&self) -> bool {
        self.1.with_duplicates
    }
    fn get_labels(&self) -> &[String] {
        &self.1.tags
    }
    fn get_query(&self) -> &[f32] {
        &self.1.vector
    }
    fn no_results(&self) -> usize {
        self.0
    }
}

pub struct VectorReaderService {
    index: Index,
    indexset: IndexSet,
}
impl Debug for VectorReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReaderService").finish()
    }
}

impl VectorReader for VectorReaderService {
    #[tracing::instrument(skip_all)]
    fn count(&self, vectorset: &str) -> NodeResult<usize> {
        let time = SystemTime::now();
        let indexet_slock = self.indexset.get_slock()?;
        if vectorset.is_empty() {
            info!("Id for the vectorset is empty");
            let index_slock = self.index.get_slock()?;
            let no_nodes = self.index.no_nodes(&index_slock);
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("Ending at {v} ms")
            }
            Ok(no_nodes)
        } else if let Some(index) = self.indexset.get(vectorset, &indexet_slock)? {
            info!("Counting nodes for {vectorset}");
            let lock = index.get_slock()?;
            let no_nodes = index.no_nodes(&lock);
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("Ending at {v} ms")
            }
            Ok(no_nodes)
        } else {
            info!("There was not a set called {vectorset}");
            if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
                info!("Ending at {v} ms")
            }
            Ok(0)
        }
    }
}
impl ReaderChild for VectorReaderService {
    type Request = VectorSearchRequest;
    type Response = VectorSearchResponse;
    fn stop(&self) -> NodeResult<()> {
        info!("Stopping vector reader Service");
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    fn search(&self, request: &Self::Request) -> NodeResult<Self::Response> {
        let id = Some(&request.id);
        let time = SystemTime::now();
        let offset = request.result_per_page * request.page_number;
        let total_to_get = offset + request.result_per_page;
        let offset = offset as usize;
        let total_to_get = total_to_get as usize;
        let indexet_slock = self.indexset.get_slock()?;
        let index_slock = self.index.get_slock()?;
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Searching: starts at {v} ms");
        }
        let result = if request.vector_set.is_empty() {
            info!("{id:?} - No vectorset specified, searching in the main index");
            self.index.search(&(total_to_get, request), &index_slock)?
        } else if let Some(index) = self.indexset.get(&request.vector_set, &indexet_slock)? {
            info!(
                "{id:?} - vectorset specified and found, searching on {}",
                request.vector_set
            );
            let lock = index.get_slock()?;
            index.search(&(total_to_get, request), &lock)?
        } else {
            info!(
                "{id:?} - A was vectorset specified, but not found. {} is not a vectorset",
                request.vector_set
            );
            vec![]
        };
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Searching: ends at {v} ms");
        }

        std::mem::drop(indexet_slock);
        std::mem::drop(index_slock);

        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating results: starts at {v} ms");
        }
        let documents = result
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx >= offset)
            .map(|(_, v)| v)
            .map(|(id, distance)| DocumentScored {
                doc_id: Some(DocumentVectorIdentifier { id }),
                score: distance,
            })
            .collect::<Vec<_>>();
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Creating results: ends at {v} ms");
        }
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("{id:?} - Ending at {v} ms")
        }
        Ok(VectorSearchResponse {
            documents,
            page_number: request.page_number,
            result_per_page: request.result_per_page,
        })
    }
    #[tracing::instrument(skip_all)]
    fn stored_ids(&self) -> Vec<String> {
        let time = SystemTime::now();
        let lock = self.index.get_slock().unwrap();
        let result = self.index.get_keys(&lock).unwrap_or_else(|err| {
            error!("Error while getting keys {err}");
            vec![]
        });
        if let Ok(v) = time.elapsed().map(|s| s.as_millis()) {
            info!("Ending at {v} ms")
        }
        result
    }
    fn reload(&self) {}
}

impl VectorReaderService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            match VectorReaderService::new(config) {
                Err(e) if path.exists() => {
                    std::fs::remove_dir(path)?;
                    Err(e)
                }
                Err(e) => Err(e),
                Ok(v) => Ok(v),
            }
        } else {
            VectorReaderService::open(config)
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn new(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        let path_indexset = std::path::Path::new(&config.vectorset);
        if path.exists() {
            Err(node_error!("Shard does exist".to_string()))
        } else {
            Ok(VectorReaderService {
                index: Index::new(path, IndexCheck::None)?,
                indexset: IndexSet::new(path_indexset, IndexCheck::None)?,
            })
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn open(config: &VectorConfig) -> NodeResult<Self> {
        let path = std::path::Path::new(&config.path);
        let path_indexset = std::path::Path::new(&config.vectorset);
        if !path.exists() {
            Err(node_error!("Shard does not exist".to_string()))
        } else {
            Ok(VectorReaderService {
                index: Index::new(path, IndexCheck::None)?,
                indexset: IndexSet::new(path_indexset, IndexCheck::None)?,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence,
    };
    use tempfile::TempDir;

    use super::*;
    use crate::service::writer::VectorWriterService;

    #[test]
    fn test_new_vector_reader() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            no_results: Some(3),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
        };
        let sentences: HashMap<String, VectorSentence> = vec![
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
            ("DOC/KEY/1/4".to_string(), vec![3.0, 5.0, 6.0]),
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
            labels: vec!["1".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let paragraphs = IndexParagraphs {
            paragraphs: HashMap::from([("DOC/KEY/1".to_string(), paragraph)]),
        };
        let resource = Resource {
            resource: Some(resource_id),
            metadata: None,
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["2".to_string()],
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
        writer.stop().unwrap();
        let reader = VectorReaderService::start(&vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            tags: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            reload: false,
            with_duplicates: true,
        };
        let result = reader.search(&request).unwrap();
        assert_eq!(result.documents.len(), 4);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            tags: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            reload: false,
            with_duplicates: false,
        };
        let result = reader.search(&request).unwrap();
        let no_nodes = reader.count("").unwrap();
        assert_eq!(no_nodes, 4);
        assert_eq!(result.documents.len(), 3);
    }
}
