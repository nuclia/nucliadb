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

use nucliadb_protos::{
    DocumentScored, DocumentVectorIdentifier, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_service_interface::prelude::*;
use tracing::*;

use crate::vectors::data_point_provider::*;

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
}
impl Debug for VectorReaderService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReaderService").finish()
    }
}

impl VectorReader for VectorReaderService {}
impl ReaderChild for VectorReaderService {
    type Request = VectorSearchRequest;
    type Response = VectorSearchResponse;
    fn stop(&self) -> InternalResult<()> {
        info!("Stopping vector reader Service");
        Ok(())
    }
    fn count(&self) -> usize {
        let lock = self.index.get_slock().unwrap();
        self.index.no_nodes(&lock)
    }
    fn search(&self, request: &Self::Request) -> InternalResult<Self::Response> {
        debug!(
            "{} {} {}",
            request.result_per_page,
            request.tags.len(),
            request.vector.len()
        );

        let lock = self.index.get_slock()?;

        let offset = request.result_per_page * request.page_number;
        let total_to_get = offset + request.result_per_page;
        let offset = offset as usize;
        let total_to_get = total_to_get as usize;

        let result = self.index.search(&(total_to_get, request), &lock)?;
        println!("{:?}", result);
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
        info!("Result created");
        Ok(VectorSearchResponse {
            documents,
            page_number: request.page_number,
            result_per_page: request.result_per_page,
        })
    }
    fn stored_ids(&self) -> Vec<String> {
        let lock = self.index.get_slock().unwrap();
        self.index.get_keys(&lock)
    }
    fn reload(&self) {}
}

impl VectorReaderService {
    pub fn start(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            VectorReaderService::new(config)
        } else {
            VectorReaderService::open(config)
        }
    }
    pub fn new(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if path.exists() {
            Err(Box::new("Shard already created".to_string()))
        } else {
            Ok(VectorReaderService {
                index: Index::reader(path)?,
            })
        }
    }
    pub fn open(config: &VectorConfig) -> InternalResult<Self> {
        let path = std::path::Path::new(&config.path);
        if !path.exists() {
            Err(Box::new("Shard does not exist".to_string()))
        } else {
            Ok(VectorReaderService {
                index: Index::reader(path)?,
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
    use crate::vectors::service::writer::VectorWriterService;

    #[test]
    fn test_new_vector_reader() {
        let dir = TempDir::new("payload_dir").unwrap();
        let vsc = VectorConfig {
            no_results: Some(3),
            path: dir.path().as_os_str().to_os_string().into_string().unwrap(),
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
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        writer.stop().unwrap();
        let reader = VectorReaderService::start(&vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
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
            vector: vec![4.0, 6.0, 7.0],
            tags: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            reload: false,
            with_duplicates: false,
        };
        let result = reader.search(&request).unwrap();
        assert_eq!(result.documents.len(), 3);
    }
}
