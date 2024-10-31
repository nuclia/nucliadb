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

use crate::data_point::{self, OpenDataPoint};
pub use crate::data_point::{DpId, Neighbour};
use crate::data_point_provider::SearchRequest;
use crate::data_point_provider::VectorConfig;
use crate::data_types::dtrie_ram::DTrie;
use crate::data_types::DeleteLog;
use crate::utils;
use crate::VectorSegmentMetadata;
use crate::{formula::*, query_io};
use crate::{VectorErr, VectorR};
use nidx_protos::prost::*;
use nidx_protos::{
    DocumentScored, DocumentVectorIdentifier, SentenceMetadata, VectorSearchRequest, VectorSearchResponse,
};
use nidx_types::query_language::*;
use nidx_types::Seq;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::time::Instant;
use tracing::*;

#[derive(Clone, Copy)]
pub struct TimeSensitiveDLog<'a> {
    pub dlog: &'a DTrie,
    pub time: Seq,
}
impl<'a> DeleteLog for TimeSensitiveDLog<'a> {
    fn is_deleted(&self, key: &[u8]) -> bool {
        self.dlog.get(key).map(|t| t > self.time).unwrap_or_default()
    }
}

// Fixed-sized sorted collection
struct Fssc {
    size: usize,
    with_duplicates: bool,
    seen: HashSet<Vec<u8>>,
    buff: HashMap<Neighbour, f32>,
}
impl From<Fssc> for Vec<Neighbour> {
    fn from(fssv: Fssc) -> Self {
        let mut result: Vec<_> = fssv.buff.into_keys().collect();
        result.sort_by(|a, b| b.score().partial_cmp(&a.score()).unwrap_or(Ordering::Less));
        result
    }
}
impl Fssc {
    fn is_full(&self) -> bool {
        self.buff.len() == self.size
    }
    fn new(size: usize, with_duplicates: bool) -> Fssc {
        Fssc {
            size,
            with_duplicates,
            seen: HashSet::new(),
            buff: HashMap::with_capacity(size),
        }
    }
    fn add(&mut self, candidate: Neighbour) {
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
    open_data_points: Vec<(OpenDataPoint, Seq)>,
    delete_log: DTrie,
    number_of_embeddings: usize,
    dimension: Option<usize>,
}

fn segment_matches(expression: &BooleanExpression, labels: &HashSet<String>) -> bool {
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

// TODO: In an ideal world this should be part of the actual request, but since
// we use protos all the way down the stack here we are. Once the protos use
// is restricted to only the upper layer, this type won't be needed anymore.
#[derive(Clone, Default)]
pub struct VectorsContext {
    pub filtering_formula: Option<BooleanExpression>,
    pub segment_filtering_formula: Option<BooleanExpression>,
}

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

impl Reader {
    pub fn open(
        segments: Vec<(VectorSegmentMetadata, Seq)>,
        config: VectorConfig,
        delete_log: DTrie,
    ) -> VectorR<Reader> {
        let mut dimension = None;
        let mut open_data_points = Vec::new();
        let mut number_of_embeddings = 0;

        for (metadata, seq) in segments {
            let open_data_point = data_point::open(metadata)?;

            number_of_embeddings += open_data_point.no_nodes();
            open_data_points.push((open_data_point, seq));
        }

        if let Some(open_data_point) = open_data_points.first() {
            dimension = open_data_point.0.stored_len();
        }

        Ok(Reader {
            config,
            open_data_points,
            delete_log,
            number_of_embeddings,
            dimension,
        })
    }

    pub fn _search(
        &self,
        request: &dyn SearchRequest,
        segment_filter: &Option<BooleanExpression>,
    ) -> VectorR<Vec<Neighbour>> {
        let normalized_query;
        let query = if self.config.normalize_vectors {
            normalized_query = utils::normalize_vector(request.get_query());
            &normalized_query
        } else {
            request.get_query()
        };

        // Validate vector dimensions
        let valid_dims = match self.config.vector_type {
            crate::config::VectorType::DenseF32Unaligned => {
                let Some(dimension) = self.dimension else {
                    return Ok(Vec::with_capacity(0));
                };
                dimension == query.len()
            }
            crate::config::VectorType::DenseF32 {
                dimension,
            } => dimension == query.len(),
        };
        if !valid_dims {
            println!("Inconsistent dimension, stored {:?}, query {}", self.config, query.len());
            return Err(VectorErr::InconsistentDimensions);
        }

        let filter = request.get_filter();
        let with_duplicates = request.with_duplicates();
        let no_results = request.no_results();
        let min_score = request.min_score();
        let mut ffsv = Fssc::new(request.no_results(), with_duplicates);

        for (open_data_point, seq) in &self.open_data_points {
            // Skip this segment if it doesn't match the segment filter
            if !segment_filter.as_ref().map_or(true, |f| segment_matches(f, open_data_point.tags())) {
                continue;
            }
            let delete_log = TimeSensitiveDLog {
                time: *seq,
                dlog: &self.delete_log,
            };
            let partial_solution = open_data_point.search(
                &delete_log,
                query,
                filter,
                with_duplicates,
                no_results,
                &self.config,
                min_score,
            );
            for candidate in partial_solution {
                ffsv.add(candidate);
            }
        }

        Ok(ffsv.into())
    }

    pub fn search(
        &self,
        request: &VectorSearchRequest,
        context: &VectorsContext,
    ) -> anyhow::Result<VectorSearchResponse> {
        let time = Instant::now();

        let id = Some(&request.id);
        let offset = request.result_per_page * request.page_number;
        let total_to_get = offset + request.result_per_page;
        let offset = offset as usize;
        let total_to_get = total_to_get as usize;

        let field_labels = request.field_labels.iter().cloned().map(AtomClause::label);
        let mut formula = Formula::new();

        if !request.field_labels.is_empty() {
            let field_atoms = field_labels.map(Clause::Atom).collect();
            let field_clause = CompoundClause::new(BooleanOperator::And, field_atoms);
            formula.extend(field_clause);
        }

        if !request.key_filters.is_empty() {
            let (field_ids, resource_ids) = request.key_filters.iter().cloned().partition(|k| k.contains('/'));
            let clause_labels = AtomClause::key_set(resource_ids, field_ids);
            formula.extend(clause_labels);
        }

        if !request.field_filters.is_empty() {
            let mut field_atoms: Vec<Clause> = vec![];
            for field in request.field_filters.iter() {
                // split "a/title" into "a" and "title"
                let parts = field.split('/').collect::<Vec<&str>>();
                let field_type = parts.first().unwrap_or(&"").to_string();
                let field_name = parts.get(1).unwrap_or(&"").to_string();
                let atom_clause = Clause::Atom(AtomClause::key_field(field_type, field_name));
                field_atoms.push(atom_clause);
            }
            let compound = CompoundClause::new(BooleanOperator::Or, field_atoms);
            formula.extend(compound);
        }

        if let Some(filter) = context.filtering_formula.as_ref() {
            let clause = query_io::map_expression(filter);
            formula.extend(clause);
        }

        let search_request = (total_to_get, request, formula);
        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");

        let result = self._search(&search_request, &context.segment_filtering_formula)?;

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

    pub fn size(&self) -> usize {
        self.number_of_embeddings
    }

    pub fn config(&self) -> &VectorConfig {
        &self.config
    }

    pub fn embedding_dimension(&self) -> Option<usize> {
        self.dimension
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use nidx_protos::resource::ResourceStatus;
    use nidx_protos::{IndexParagraph, IndexParagraphs, Resource, ResourceId, VectorSentence, VectorsetSentences};
    use tempfile::TempDir;

    use super::*;
    use crate::config::{Similarity, VectorConfig, VectorType};
    use crate::indexer::{index_resource, ResourceWrapper};

    #[test]
    fn test_key_prefix_search() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 {
                dimension: 3,
            },
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([(
                "__default__".to_string(),
                VectorsetSentences {
                    sentences,
                },
            )]),
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

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?;

        let reader = Reader::open(vec![(segment.unwrap(), 0i64.into())], vsc, DTrie::new()).unwrap();
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

        Ok(())
    }

    #[test]
    fn test_new_vector_reader() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 {
                dimension: 3,
            },
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([(
                "__default__".to_string(),
                VectorsetSentences {
                    sentences,
                },
            )]),
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

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?;

        let reader = Reader::open(vec![(segment.unwrap(), 0i64.into())], vsc, DTrie::new()).unwrap();
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

        Ok(())
    }

    #[test]
    fn test_vectors_deduplication() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 {
                dimension: 3,
            },
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
            sentences: sentences.clone(),
            vectorsets_sentences: HashMap::from([(
                "__default__".to_string(),
                VectorsetSentences {
                    sentences,
                },
            )]),
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

        let segment = index_resource(ResourceWrapper::from(&resource), segment_path, &vsc)?;

        let reader = Reader::open(vec![(segment.unwrap(), 0i64.into())], vsc, DTrie::new()).unwrap();
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

        Ok(())
    }
}
