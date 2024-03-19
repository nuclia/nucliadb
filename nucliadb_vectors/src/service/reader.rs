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
use std::time::Instant;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::prost::Message;
use nucliadb_core::protos::{
    DocumentScored, DocumentVectorIdentifier, SentenceMetadata, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::*;
use nucliadb_procs::measure;

use crate::data_point_provider::*;
use crate::formula::{AtomClause, BooleanOperator, Clause, CompoundClause, Formula};
use crate::indexset::IndexSet;
use crate::service::query_io;

impl<'a> SearchRequest for (usize, &'a VectorSearchRequest, Formula) {
    fn with_duplicates(&self) -> bool {
        self.1.with_duplicates
    }
    fn get_filter(&self) -> &Formula {
        &self.2
    }
    fn get_query(&self) -> &[f32] {
        &self.1.vector
    }
    fn no_results(&self) -> usize {
        self.0
    }
    fn min_score(&self) -> f32 {
        self.1.min_score
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
        if vectorset.is_empty() {
            debug!("Id for the vectorset is empty");
            self.index_count(&self.index)
        } else {
            let indexet_slock = self.indexset.get_slock()?;
            if let Some(index) = self.indexset.get(vectorset, &indexet_slock)? {
                debug!("Counting nodes for {vectorset}");
                self.index_count(&index)
            } else {
                debug!("There was not a set called {vectorset}");
                Ok(0)
            }
        }
    }

    #[tracing::instrument(skip_all)]
    fn update(&mut self) -> NodeResult<()> {
        Ok(())
    }

    #[measure(actor = "vectors", metric = "search")]
    #[tracing::instrument(skip_all)]
    fn search(&self, request: &ProtosRequest, context: &VectorsContext) -> NodeResult<ProtosResponse> {
        let time = Instant::now();

        let id = Some(&request.id);
        let offset = request.result_per_page * request.page_number;
        let total_to_get = offset + request.result_per_page;
        let offset = offset as usize;
        let total_to_get = total_to_get as usize;
        let indexet_slock = self.indexset.get_slock()?;
        let index_slock = self.index.get_slock()?;

        let key_filters = request.key_filters.iter().cloned().map(AtomClause::key_prefix);
        let field_labels = request.field_labels.iter().cloned().map(AtomClause::label);
        let mut formula = Formula::new();

        if !request.field_labels.is_empty() {
            let field_atoms = field_labels.map(Clause::Atom).collect();
            let field_clause = CompoundClause::new(BooleanOperator::And, field_atoms);
            formula.extend(field_clause);
        }

        if key_filters.len() > 0 {
            let clause_labels = key_filters.map(Clause::Atom).collect();
            let key_clause = CompoundClause::new(BooleanOperator::Or, clause_labels);
            formula.extend(key_clause);
        }

        if let Some(filter) = context.filtering_formula.as_ref() {
            let clause = query_io::map_expression(filter);
            formula.extend(clause);
        }

        let search_request = (total_to_get, request, formula);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let result = if request.vector_set.is_empty() {
            debug!("{id:?} - No vectorset specified, searching in the main index");
            self.index.search(&search_request, &index_slock)?
        } else if let Some(index) = self.indexset.get(&request.vector_set, &indexet_slock)? {
            debug!("{id:?} - vectorset specified and found, searching on {}", request.vector_set);
            let lock = index.get_slock()?;
            index.search(&search_request, &lock)?
        } else {
            debug!("{id:?} - A was vectorset specified, but not found. {} is not a vectorset", request.vector_set);
            vec![]
        };
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");

        std::mem::drop(indexet_slock);
        std::mem::drop(index_slock);

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating results: starts at {v} ms");

        let documents = result
            .into_iter()
            .enumerate()
            .filter(|(idx, _)| *idx >= offset)
            .map(|(_, v)| v)
            .flat_map(DocumentScored::try_from)
            .collect::<Vec<_>>();
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Creating results: ends at {v} ms");

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(VectorSearchResponse {
            documents,
            page_number: request.page_number,
            result_per_page: request.result_per_page,
        })
    }

    #[measure(actor = "vectors", metric = "stored_ids")]
    #[tracing::instrument(skip_all)]
    fn stored_ids(&self) -> NodeResult<Vec<String>> {
        let time = Instant::now();
        let lock = self.index.get_slock().unwrap();
        let result = self.index.get_keys(&lock)?;
        let v = time.elapsed().as_millis();
        debug!("Ending at {v} ms");

        Ok(result)
    }
}

impl TryFrom<Neighbour> for DocumentScored {
    type Error = String;
    fn try_from(neighbour: Neighbour) -> Result<Self, Self::Error> {
        let id = std::str::from_utf8(neighbour.id());
        let metadata = neighbour.metadata().map(SentenceMetadata::decode);
        let labels = neighbour.labels();
        let Ok(id) = id.map(|i| i.to_string()) else {
            return Err("Id could not be decoded".to_string());
        };
        let Ok(metadata) = metadata.transpose() else {
            return Err("The metadata could not be decoded".to_string());
        };
        Ok(DocumentScored {
            labels,
            metadata,
            doc_id: Some(DocumentVectorIdentifier {
                id,
            }),
            score: neighbour.score(),
        })
    }
}

impl VectorReaderService {
    #[tracing::instrument(skip_all)]
    pub fn start(config: &VectorConfig) -> NodeResult<Self> {
        if !config.path.exists() {
            return Err(node_error!("Invalid path {:?}", config.path));
        }
        if !config.vectorset.exists() {
            return Err(node_error!("Invalid path {:?}", config.vectorset));
        }
        Ok(VectorReaderService {
            index: Index::open(&config.path)?,
            indexset: IndexSet::new(&config.vectorset)?,
        })
    }

    #[measure(actor = "vectors", metric = "count")]
    fn index_count(&self, index: &Index) -> NodeResult<usize> {
        let lock = index.get_slock()?;
        let no_nodes = index.no_nodes(&lock);
        std::mem::drop(lock);
        Ok(no_nodes)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nucliadb_core::protos::resource::ResourceStatus;
    use nucliadb_core::protos::{
        IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence, VectorSimilarity,
    };
    use nucliadb_core::Channel;
    use tempfile::TempDir;

    use super::*;
    use crate::service::writer::VectorWriterService;

    #[test]
    fn test_key_prefix_search() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
            channel: Channel::EXPERIMENTAL,
        };
        let raw_sentences = [
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
            ("DOC/KEY/1/4".to_string(), vec![3.0, 5.0, 6.0]),
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
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };
        // insert - delete - insert sequence
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());

        let reader = VectorReaderService::start(&vsc).unwrap();
        let mut request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            key_filters: vec!["DOC/KEY/1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let context = VectorsContext::default();
        let result = reader.search(&request, &context).unwrap();
        assert_eq!(result.documents.len(), 4);

        request.key_filters = vec!["DOC/KEY/0".to_string()];
        let result = reader.search(&request, &context).unwrap();
        assert_eq!(result.documents.len(), 0);
    }

    #[test]
    fn test_new_vector_reader() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
            channel: Channel::EXPERIMENTAL,
        };
        let raw_sentences = [
            ("DOC/KEY/1/1".to_string(), vec![1.0, 3.0, 4.0]),
            ("DOC/KEY/1/2".to_string(), vec![2.0, 4.0, 5.0]),
            ("DOC/KEY/1/3".to_string(), vec![3.0, 5.0, 6.0]),
            ("DOC/KEY/1/4".to_string(), vec![3.0, 5.0, 6.0]),
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
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };
        // insert - delete - insert sequence
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        let reader = VectorReaderService::start(&vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let context = VectorsContext::default();
        let result = reader.search(&request, &context).unwrap();
        assert_eq!(result.documents.len(), 4);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = reader.search(&request, &context).unwrap();
        let no_nodes = reader.count("").unwrap();
        assert_eq!(no_nodes, 4);
        assert_eq!(result.documents.len(), 3);

        // Check that min_score works
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            min_score: 900.0,
            ..Default::default()
        };
        let result = reader.search(&request, &context).unwrap();
        let no_nodes = reader.count("").unwrap();
        assert_eq!(no_nodes, 4);
        assert_eq!(result.documents.len(), 0);

        let bad_request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        assert!(reader.search(&bad_request, &context).is_err());
    }

    #[test]
    fn test_vectors_deduplication() {
        let dir = TempDir::new().unwrap();
        let vsc = VectorConfig {
            similarity: Some(VectorSimilarity::Cosine),
            path: dir.path().join("vectors"),
            vectorset: dir.path().join("vectorset"),
            channel: Channel::EXPERIMENTAL,
        };
        let raw_sentences =
            [("DOC/KEY/1/1".to_string(), vec![1.0, 2.0, 3.0]), ("DOC/KEY/1/2".to_string(), vec![1.0, 2.0, 3.0])];
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
            relations: vec![],
            vectors: HashMap::default(),
            vectors_to_delete: HashMap::default(),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };
        let mut writer = VectorWriterService::start(&vsc).unwrap();
        let res = writer.set_resource(&resource);
        assert!(res.is_ok());
        let reader = VectorReaderService::start(&vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let context = VectorsContext::default();
        let result = reader.search(&request, &context).unwrap();
        assert_eq!(result.documents.len(), 2);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            field_labels: vec!["1".to_string()],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = reader.search(&request, &context).unwrap();
        assert_eq!(result.documents.len(), 1);
    }
}
