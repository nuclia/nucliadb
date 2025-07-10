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
//

pub use crate::data_point::Neighbour;
use crate::data_point::OpenDataPoint;
use crate::data_point_provider::SearchRequest;
use crate::data_point_provider::VectorConfig;
use crate::request_types::VectorSearchRequest;
use crate::utils;
use crate::{VectorErr, VectorR};
use crate::{formula::*, query_io};
use nidx_protos::prost::*;
use nidx_protos::{DocumentScored, DocumentVectorIdentifier, SentenceMetadata, VectorSearchResponse};
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::*;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tracing::*;

// Fixed-sized sorted collection
struct Fssc<'a> {
    size: usize,
    with_duplicates: bool,
    seen: HashSet<Vec<u8>>,
    buff: HashMap<Neighbour<'a>, f32>,
}
impl<'a> From<Fssc<'a>> for Vec<Neighbour<'a>> {
    fn from(fssv: Fssc<'a>) -> Self {
        let mut result: Vec<_> = fssv.buff.into_keys().collect();
        result.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap_or(Ordering::Less));
        result
    }
}
impl<'a> Fssc<'a> {
    fn is_full(&self) -> bool {
        self.buff.len() == self.size
    }
    fn new(size: usize, with_duplicates: bool) -> Fssc<'a> {
        Fssc {
            size,
            with_duplicates,
            seen: HashSet::new(),
            buff: HashMap::with_capacity(size),
        }
    }
    fn add(&mut self, candidate: Neighbour<'a>) {
        if !self.with_duplicates && self.seen.contains(candidate.vector()) {
            return;
        } else if !self.with_duplicates {
            let vector = candidate.vector().to_vec();
            self.seen.insert(vector);
        }

        let score = candidate.score();
        if self.is_full() {
            let smallest_bigger = self
                .buff
                .iter()
                .map(|(key, score)| (key.clone(), *score))
                .filter(|(_, v)| score > *v)
                .min_by(|(_, v0), (_, v1)| v0.partial_cmp(v1).unwrap())
                .map(|(key, _)| key);
            if let Some(key) = smallest_bigger {
                self.buff.remove_entry(&key);
                self.buff.insert(candidate, score);
            }
        } else {
            self.buff.insert(candidate, score);
        }
    }
}

pub struct Reader {
    config: VectorConfig,
    open_data_points: Vec<OpenDataPoint>,
}

fn segment_matches(expression: &BooleanExpression<String>, labels: &HashSet<String>) -> bool {
    match expression {
        BooleanExpression::Literal(tag) => labels.contains(tag),
        BooleanExpression::Not(expr) => !segment_matches(expr, labels),
        BooleanExpression::Operation(BooleanOperation {
            operator: Operator::And,
            operands,
        }) => operands.iter().all(|op| segment_matches(op, labels)),
        BooleanExpression::Operation(BooleanOperation {
            operator: Operator::Or,
            operands,
        }) => operands.iter().any(|op| segment_matches(op, labels)),
    }
}

impl SearchRequest for (usize, &VectorSearchRequest, Formula) {
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

impl TryFrom<Neighbour<'_>> for DocumentScored {
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
            doc_id: Some(DocumentVectorIdentifier { id }),
            score: neighbour.score(),
        })
    }
}

impl Reader {
    pub fn open(open_data_points: Vec<OpenDataPoint>, config: VectorConfig) -> VectorR<Reader> {
        Ok(Reader {
            config,
            open_data_points,
        })
    }

    pub fn space_usage(&self) -> usize {
        self.open_data_points.iter().map(|dp| dp.space_usage()).sum()
    }

    pub fn _search(
        &self,
        request: &dyn SearchRequest,
        segment_filter: &Option<BooleanExpression<String>>,
    ) -> VectorR<Vec<Neighbour>> {
        let normalized_query;
        let query = if self.config.normalize_vectors {
            normalized_query = utils::normalize_vector(request.get_query());
            &normalized_query
        } else {
            request.get_query()
        };

        // Validate vector dimensions
        let crate::config::VectorType::DenseF32 { dimension } = self.config.vector_type;

        if dimension != query.len() {
            return Err(VectorErr::InconsistentDimensions {
                index_config: dimension,
                vector: query.len(),
            });
        }

        let filter = request.get_filter();
        let with_duplicates = request.with_duplicates();
        let no_results = request.no_results();
        let min_score = request.min_score();
        let mut ffsv = Fssc::new(request.no_results(), with_duplicates);

        for open_data_point in &self.open_data_points {
            // Skip this segment if it doesn't match the segment filter
            if !segment_filter
                .as_ref()
                .is_none_or(|f| segment_matches(f, open_data_point.tags()))
            {
                continue;
            }
            let partial_solution =
                open_data_point.search(query, filter, with_duplicates, no_results, &self.config, min_score);
            for candidate in partial_solution {
                ffsv.add(candidate);
            }
        }

        Ok(ffsv.into())
    }

    pub fn search(
        &self,
        request: &VectorSearchRequest,
        prefilter: &PrefilterResult,
    ) -> anyhow::Result<VectorSearchResponse> {
        let time = Instant::now();

        let id = Some(&request.id);
        let offset = request.result_per_page * request.page_number;
        let total_to_get = offset + request.result_per_page;
        let offset = offset as usize;
        let total_to_get = total_to_get as usize;

        let mut formula = Formula::new();

        if let PrefilterResult::Some(valid_fields) = prefilter {
            let field_ids = valid_fields
                .iter()
                .map(|f| format!("{}{}", f.resource_id.simple(), f.field_id))
                .collect();
            let clause_labels = AtomClause::key_set(field_ids);
            formula.extend(clause_labels);
        }

        if let Some(filter) = request.filtering_formula.as_ref() {
            let clause = query_io::map_expression(filter);
            formula.extend(clause);
        }

        if request.filter_or {
            formula.operator = BooleanOperator::Or;
        }

        let search_request = (total_to_get, request, formula);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let result = self._search(&search_request, &request.segment_filtering_formula)?;

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");
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
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nidx_protos::resource::ResourceStatus;
    use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence, VectorsetSentences};
    use nidx_types::prefilter::FieldId;
    use tempfile::TempDir;

    use super::*;
    use crate::config::{Similarity, VectorConfig, VectorType};
    use crate::data_point;
    use crate::indexer::{ResourceWrapper, index_resource};

    #[test]
    fn test_key_prefix_search() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 { dimension: 3 },
        };
        let raw_sentences = [
            (
                "6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1/1".to_string(),
                vec![1.0, 3.0, 4.0],
            ),
            (
                "6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1/2".to_string(),
                vec![2.0, 4.0, 5.0],
            ),
            (
                "6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1/3".to_string(),
                vec![3.0, 5.0, 6.0],
            ),
            (
                "6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1/4".to_string(),
                vec![3.0, 5.0, 6.0],
            ),
        ];
        let resource_id = ResourceId {
            shard_id: "DOC".to_string(),
            uuid: "6c5fc1f7a69042d4b24b7f18ea354b4a/f/field".to_string(),
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([("__default__".to_string(), VectorsetSentences { sentences })]),
            field: "".to_string(),
            labels: vec!["1".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let paragraphs = IndexParagraphs {
            paragraphs: HashMap::from([("6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1".to_string(), paragraph)]),
        };
        let resource = Resource {
            resource: Some(resource_id),
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["2".to_string()],
            paragraphs: HashMap::from([("6c5fc1f7a69042d4b24b7f18ea354b4a/f/field1".to_string(), paragraphs)]),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?.unwrap();

        let reader = Reader::open(vec![data_point::open(segment)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let mut field_filter = FieldId {
            resource_id: uuid::Uuid::parse_str("6c5fc1f7a69042d4b24b7f18ea354b4a").unwrap(),
            field_id: "/f/field1".to_string(),
        };
        let result = reader
            .search(&request, &PrefilterResult::Some(vec![field_filter.clone()]))
            .unwrap();
        assert_eq!(result.documents.len(), 4);

        field_filter.field_id = "/f/field2".to_string();
        let result = reader
            .search(&request, &PrefilterResult::Some(vec![field_filter]))
            .unwrap();
        assert_eq!(result.documents.len(), 0);

        Ok(())
    }

    #[test]
    fn test_new_vector_reader() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 { dimension: 3 },
        };
        let raw_sentences = [
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-1".to_string(),
                vec![1.0, 3.0, 4.0],
            ),
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/1-2".to_string(),
                vec![2.0, 4.0, 5.0],
            ),
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/2-3".to_string(),
                vec![3.0, 5.0, 6.0],
            ),
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/3-4".to_string(),
                vec![3.0, 5.0, 6.0],
            ),
        ];
        let resource_id = ResourceId {
            shard_id: "dec1c64b-06e5-419c-897a-72d8a39b799e".to_string(),
            uuid: "9cb39c75f8d9498d8f82d92b173011f5".to_string(),
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([("__default__".to_string(), VectorsetSentences { sentences })]),
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
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["2".to_string()],
            paragraphs: HashMap::from([("DOC/KEY".to_string(), paragraphs)]),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?.unwrap();

        let reader = Reader::open(vec![data_point::open(segment)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let result = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 4);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 3);

        // Check that min_score works
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            min_score: 900.0,
            ..Default::default()
        };
        let result = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 0);

        let bad_request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        assert!(reader.search(&bad_request, &PrefilterResult::All).is_err());

        Ok(())
    }

    #[test]
    fn test_vectors_deduplication() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 { dimension: 3 },
        };
        let raw_sentences = [
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string(),
                vec![1.0, 2.0, 3.0],
            ),
            (
                "9cb39c75f8d9498d8f82d92b173011f5/f/field/100-200".to_string(),
                vec![1.0, 2.0, 3.0],
            ),
        ];
        let resource_id = ResourceId {
            shard_id: "dec1c64b-06e5-419c-897a-72d8a39b799e".to_string(),
            uuid: "9cb39c75f8d9498d8f82d92b173011f5".to_string(),
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([("__default__".to_string(), VectorsetSentences { sentences })]),
            field: "".to_string(),
            labels: vec!["1".to_string()],
            index: 3,
            split: "".to_string(),
            repeated_in_field: false,
            metadata: None,
        };
        let paragraphs = IndexParagraphs {
            paragraphs: HashMap::from([("9cb39c75f8d9498d8f82d92b173011f5/f/field/0-100".to_string(), paragraph)]),
        };
        let resource = Resource {
            resource: Some(resource_id),
            texts: HashMap::with_capacity(0),
            status: ResourceStatus::Processed as i32,
            labels: vec!["2".to_string()],
            paragraphs: HashMap::from([("f/field".to_string(), paragraphs)]),
            shard_id: "DOC".to_string(),
            ..Default::default()
        };

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?.unwrap();

        let reader = Reader::open(vec![data_point::open(segment)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let result = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 2);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            page_number: 0,
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = reader.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 1);

        Ok(())
    }
}
