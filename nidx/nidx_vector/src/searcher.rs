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

use crate::ParagraphAddr;
use crate::config::VectorCardinality;
use crate::config::VectorConfig;
use crate::data_store::ParagraphRef;
use crate::multivector::extract_multi_vectors;
use crate::multivector::maxsim_similarity;
use crate::request_types::VectorSearchRequest;
use crate::segment::OpenSegment;
use crate::utils;
use crate::{VectorErr, VectorR};
use crate::{formula::*, query_io};
use nidx_protos::prost::*;
use nidx_protos::{DocumentScored, DocumentVectorIdentifier, SentenceMetadata, VectorSearchResponse};
use nidx_types::prefilter::PrefilterResult;
use nidx_types::query_language::*;
use rayon::prelude::*;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::time::Instant;
use tracing::*;

struct SearchRequest<'a> {
    request: &'a VectorSearchRequest,
    formula: Formula,
}

impl SearchRequest<'_> {
    fn with_duplicates(&self) -> bool {
        self.request.with_duplicates
    }
    fn get_filter(&self) -> &Formula {
        &self.formula
    }
    fn get_query(&self) -> &[f32] {
        &self.request.vector
    }
    fn no_results(&self) -> usize {
        self.request.result_per_page as usize
    }
    fn min_score(&self) -> f32 {
        self.request.min_score
    }
}

#[derive(Clone)]
pub struct ScoredParagraph<'a> {
    score: f32,
    paragraph: ParagraphRef<'a>,
    address: ParagraphAddr,
    segment: &'a OpenSegment,
}
impl Eq for ScoredParagraph<'_> {}
impl std::hash::Hash for ScoredParagraph<'_> {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.id().hash(state)
    }

    fn hash_slice<H: std::hash::Hasher>(data: &[Self], state: &mut H)
    where
        Self: Sized,
    {
        for piece in data {
            piece.hash(state)
        }
    }
}
impl Ord for ScoredParagraph<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.paragraph.id().cmp(other.paragraph.id())
    }
}
impl PartialOrd for ScoredParagraph<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl PartialEq for ScoredParagraph<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.paragraph.id() == other.paragraph.id()
    }
}

impl<'a> ScoredParagraph<'a> {
    pub fn new(segment: &'a OpenSegment, address: ParagraphAddr, paragraph: ParagraphRef<'a>, score: f32) -> Self {
        Self {
            segment,
            paragraph,
            score,
            address,
        }
    }
    pub fn score(&self) -> f32 {
        self.score
    }
    pub fn id(&self) -> &str {
        self.paragraph.id()
    }
    pub fn labels(&self) -> Vec<String> {
        self.paragraph.labels()
    }
    pub fn metadata(&self) -> Option<&[u8]> {
        let metadata = self.paragraph.metadata();
        (!metadata.is_empty()).then_some(metadata)
    }
    pub fn vectors(&self) -> Vec<&[u8]> {
        self.paragraph
            .vectors(&self.address)
            .map(|va| self.segment.get_vector(va).vector())
            .collect()
    }
}

// Fixed-sized sorted collection
struct Fssc<'a> {
    size: usize,
    with_duplicates: bool,
    seen: HashSet<Vec<u8>>,
    buff: HashSet<ScoredParagraph<'a>>,
}
impl<'a> From<Fssc<'a>> for Vec<ScoredParagraph<'a>> {
    fn from(fssv: Fssc<'a>) -> Self {
        let mut result: Vec<_> = fssv.buff.into_iter().collect();
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
            buff: HashSet::with_capacity(size),
        }
    }
    fn add(&mut self, candidate: ScoredParagraph<'a>, vector: &[u8]) {
        if !self.with_duplicates && self.seen.contains(vector) {
            return;
        } else if !self.with_duplicates {
            let vector = vector.to_vec();
            self.seen.insert(vector);
        }

        let score = candidate.score();
        if self.is_full() {
            let smallest_bigger = self
                .buff
                .iter()
                .filter(|sp| score > sp.score())
                .min_by(|sp0, sp1| sp0.score().partial_cmp(&sp1.score()).unwrap())
                .cloned();
            if let Some(key) = smallest_bigger {
                self.buff.remove(&key);
                self.buff.insert(candidate);
            }
        } else {
            self.buff.insert(candidate);
        }
    }
}

pub struct Searcher {
    config: VectorConfig,
    open_segments: Vec<OpenSegment>,
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

impl TryFrom<ScoredParagraph<'_>> for DocumentScored {
    type Error = String;
    fn try_from(paragraph: ScoredParagraph) -> Result<Self, Self::Error> {
        let id = paragraph.id().to_string();
        let metadata = paragraph.metadata().map(SentenceMetadata::decode);
        let labels = paragraph.labels();
        let Ok(metadata) = metadata.transpose() else {
            return Err("The metadata could not be decoded".to_string());
        };
        Ok(DocumentScored {
            labels,
            metadata,
            doc_id: Some(DocumentVectorIdentifier { id }),
            score: paragraph.score(),
        })
    }
}

impl Searcher {
    pub fn open(open_data_points: Vec<OpenSegment>, config: VectorConfig) -> VectorR<Searcher> {
        Ok(Searcher {
            config,
            open_segments: open_data_points,
        })
    }

    pub fn space_usage(&self) -> usize {
        self.open_segments.iter().map(|dp| dp.space_usage()).sum()
    }

    fn _search(
        &self,
        request: &SearchRequest,
        segment_filter: &Option<BooleanExpression<String>>,
    ) -> VectorR<Vec<ScoredParagraph>> {
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

        for open_segment in &self.open_segments {
            // Skip this segment if it doesn't match the segment filter
            if !segment_filter
                .as_ref()
                .is_none_or(|f| segment_matches(f, open_segment.tags()))
            {
                continue;
            }
            let partial_solution =
                open_segment.search(query, filter, with_duplicates, no_results, &self.config, min_score);

            for candidate in partial_solution {
                let addr = candidate.paragraph();
                let paragraph = open_segment.get_paragraph(addr);
                let scored_paragraph = ScoredParagraph::new(open_segment, addr, paragraph, candidate.score());
                ffsv.add(scored_paragraph, candidate.vector());
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

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: starts at {v} ms");
        let search_request = &SearchRequest { request, formula };
        let result = match self.config.vector_cardinality {
            VectorCardinality::Single => self._search(search_request, &request.segment_filtering_formula)?,
            VectorCardinality::Multi => self.search_multi_vector(search_request)?,
        };

        let v = time.elapsed().as_millis();
        debug!("{id:?} - Searching: ends at {v} ms");

        debug!("{id:?} - Creating results: starts at {v} ms");

        let documents = result
            .into_iter()
            .flat_map(DocumentScored::try_from)
            .collect::<Vec<_>>();
        let v = time.elapsed().as_millis();

        debug!("{id:?} - Creating results: ends at {v} ms");

        let took = time.elapsed().as_secs_f64();
        debug!("{id:?} - Ending at {took} ms");

        Ok(VectorSearchResponse {
            documents,
            result_per_page: request.result_per_page,
        })
    }

    fn search_multi_vector(&self, search: &SearchRequest) -> VectorR<Vec<ScoredParagraph>> {
        let search_vectors = extract_multi_vectors(&search.request.vector, &self.config.vector_type)?;
        let encoded_query = search_vectors
            .iter()
            .map(|v| self.config.vector_type.encode(v))
            .collect::<Vec<_>>();

        // Search for each vector in the query
        let results = search_vectors
            .into_par_iter()
            .map(|v| {
                let mut request = search.request.clone();

                request.vector = v;
                // We are OK with duplicate individual vectors. We always deduplicate by paragraphs anyway (NodeFilter.paragraphs)
                request.with_duplicates = true;
                // We don't care about min_score in this first pass, we apply min_score on top of maxsim similarity
                request.min_score = f32::MIN;
                // Request at least a few vectors, since the rerank may offer different results later
                request.result_per_page = search.request.result_per_page.max(10);

                let search_request = SearchRequest {
                    request: &request,
                    formula: search.formula.clone(),
                };
                self._search(&search_request, &search.request.segment_filtering_formula)
            })
            .collect::<Result<Vec<_>, _>>()?;

        // Remove duplicates, we only want each paragraph once
        let mut result_paragraphs = results.into_iter().flatten().collect::<Vec<_>>();
        result_paragraphs.sort_unstable_by_key(|rp| rp.address);
        result_paragraphs.dedup_by_key(|rp| rp.address);

        // Score each paragraph using maxsim
        let similarity_function = self.config.similarity_function();
        let mut results = result_paragraphs
            .into_par_iter()
            .filter_map(|mut sp| {
                sp.score = maxsim_similarity(similarity_function, &encoded_query, &sp.vectors());
                (sp.score() > search.request.min_score).then_some(sp)
            })
            .collect::<Vec<_>>();

        // Select top_k
        results.sort_unstable_by(|a, b| b.score().partial_cmp(&a.score()).unwrap());
        results.truncate(search.request.result_per_page as usize);

        Ok(results)
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
    use crate::config::{Similarity, VectorCardinality, VectorConfig, VectorType};
    use crate::indexer::{ResourceWrapper, index_resource};
    use crate::segment;

    #[test]
    fn test_key_prefix_search() -> anyhow::Result<()> {
        let dir = TempDir::new().unwrap();
        let segment_path = dir.path();
        let vsc = VectorConfig {
            similarity: Similarity::Cosine,
            normalize_vectors: false,
            vector_type: VectorType::DenseF32 { dimension: 3 },
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
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

        let searcher = Searcher::open(vec![segment::open(segment, &vsc)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let mut field_filter = FieldId {
            resource_id: uuid::Uuid::parse_str("6c5fc1f7a69042d4b24b7f18ea354b4a").unwrap(),
            field_id: "/f/field1".to_string(),
        };
        let result = searcher
            .search(&request, &PrefilterResult::Some(vec![field_filter.clone()]))
            .unwrap();
        assert_eq!(result.documents.len(), 4);

        field_filter.field_id = "/f/field2".to_string();
        let result = searcher
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
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
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

        let searcher = Searcher::open(vec![segment::open(segment, &vsc)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let result = searcher.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 4);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = searcher.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 3);

        // Check that min_score works
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: false,
            min_score: 900.0,
            ..Default::default()
        };
        let result = searcher.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 0);

        let bad_request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0],
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        assert!(searcher.search(&bad_request, &PrefilterResult::All).is_err());

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
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
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

        let searcher = Searcher::open(vec![segment::open(segment, &vsc)?], vsc).unwrap();
        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: true,
            ..Default::default()
        };
        let result = searcher.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 2);

        let request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: "".to_string(),
            vector: vec![4.0, 6.0, 7.0],
            result_per_page: 20,
            with_duplicates: false,
            ..Default::default()
        };
        let result = searcher.search(&request, &PrefilterResult::All).unwrap();
        assert_eq!(result.documents.len(), 1);

        Ok(())
    }
}
