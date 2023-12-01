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
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};

use crossbeam_utils::thread as crossbeam_thread;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_core::protos::{
    DocumentSearchRequest, DocumentSearchResponse, EdgeList, GetShardRequest,
    ParagraphSearchRequest, ParagraphSearchResponse, RelatedEntities, RelationPrefixSearchRequest,
    RelationSearchRequest, RelationSearchResponse, SearchRequest, SearchResponse, Shard, ShardFile,
    ShardFileChunk, ShardFileList, StreamRequest, SuggestFeatures, SuggestRequest, SuggestResponse,
    TypeList, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_core::query_planner::QueryPlan;
use nucliadb_core::thread::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_procs::measure;

use crate::disk_structure::*;
use crate::shards::metadata::ShardMetadata;
use crate::shards::versions::Versions;
use crate::telemetry::run_with_telemetry;

const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;
const MIN_VIABLE_PREFIX_SUGGEST: usize = 1;
const CHUNK_SIZE: usize = 65535;

pub struct ShardFileChunkIterator {
    file_path: PathBuf,
    reader: BufReader<File>,
    chunk_size: usize,
    idx: i32,
}

impl ShardFileChunkIterator {
    pub fn new(file_path: PathBuf, chunk_size: usize) -> NodeResult<ShardFileChunkIterator> {
        let file = File::open(file_path.clone())?;
        let reader = BufReader::new(file);
        Ok(ShardFileChunkIterator {
            file_path,
            reader,
            chunk_size,
            idx: 0,
        })
    }
}

impl Iterator for ShardFileChunkIterator {
    type Item = ShardFileChunk;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = vec![0; self.chunk_size];

        match self.reader.read(&mut buffer) {
            Ok(0) => None, // End of file
            Ok(bytes_read) => {
                buffer.truncate(bytes_read);
                let chunk = ShardFileChunk {
                    data: buffer,
                    index: self.idx,
                };
                self.idx += 1;
                Some(chunk)
            }
            Err(e) => {
                warn!("Error reading file {:?} - {:?}", self.file_path, e);
                None
            }
        }
    }
}

#[derive(Debug)]
pub struct ShardReader {
    pub id: String,
    pub metadata: ShardMetadata,
    root_path: PathBuf,
    suffixed_root_path: String,
    text_reader: TextsReaderPointer,
    paragraph_reader: ParagraphsReaderPointer,
    vector_reader: VectorsReaderPointer,
    relation_reader: RelationsReaderPointer,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardReader {
    #[tracing::instrument(skip_all)]
    pub fn text_version(&self) -> DocumentService {
        match self.document_service_version {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            i => panic!("Unknown document version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_version(&self) -> ParagraphService {
        match self.paragraph_service_version {
            0 => ParagraphService::ParagraphV0,
            1 => ParagraphService::ParagraphV1,
            i => panic!("Unknown paragraph version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_version(&self) -> VectorService {
        match self.vector_service_version {
            0 => VectorService::VectorV0,
            1 => VectorService::VectorV1,
            i => panic!("Unknown vector version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn relation_version(&self) -> RelationService {
        match self.relation_service_version {
            0 => RelationService::RelationV0,
            1 => RelationService::RelationV1,
            2 => RelationService::RelationV2,
            i => panic!("Unknown relation version {i}"),
        }
    }

    #[measure(actor = "shard", metric = "get_info")]
    #[tracing::instrument(skip_all)]
    pub fn get_info(&self, request: &GetShardRequest) -> NodeResult<Shard> {
        let span = tracing::Span::current();

        let paragraphs = self.paragraph_reader.clone();
        let vectors = self.vector_reader.clone();
        let texts = self.text_reader.clone();

        let info = info_span!(parent: &span, "text count");
        let text_task = || run_with_telemetry(info, move || texts.count());
        let info = info_span!(parent: &span, "paragraph count");
        let paragraph_task = || run_with_telemetry(info, move || paragraphs.count());
        let info = info_span!(parent: &span, "vector count");
        let vector_task = || run_with_telemetry(info, move || vectors.count(&request.vectorset));

        let mut text_result = Ok(0);
        let mut paragraph_result = Ok(0);
        let mut vector_result = Ok(0);
        crossbeam_thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
        })
        .expect("Failed to join threads");

        Ok(Shard {
            metadata: Some(self.metadata.clone().into()),
            shard_id: self.id.clone(),
            // naming issue here, this is not number of resource
            // but more like number of fields
            fields: text_result? as u64,
            paragraphs: paragraph_result? as u64,
            sentences: vector_result? as u64,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn get_text_keys(&self) -> NodeResult<Vec<String>> {
        self.text_reader.stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_paragraphs_keys(&self) -> NodeResult<Vec<String>> {
        self.paragraph_reader.stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_vectors_keys(&self) -> NodeResult<Vec<String>> {
        self.vector_reader.stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_relations_keys(&self) -> NodeResult<Vec<String>> {
        self.relation_reader.stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_relations_edges(&self) -> NodeResult<EdgeList> {
        self.relation_reader.get_edges()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_relations_types(&self) -> NodeResult<TypeList> {
        self.relation_reader.get_node_types()
    }

    #[measure(actor = "shard", metric = "new")]
    #[tracing::instrument(skip_all)]
    pub fn new(id: String, shard_path: &Path) -> NodeResult<ShardReader> {
        let span = tracing::Span::current();

        let metadata = ShardMetadata::open(&shard_path.join(METADATA_FILE))?;
        let tsc = TextConfig {
            path: shard_path.join(TEXTS_DIR),
        };

        let psc: ParagraphConfig = ParagraphConfig {
            path: shard_path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel.unwrap_or_default();

        let vsc = VectorConfig {
            similarity: None,
            path: shard_path.join(VECTORS_DIR),
            vectorset: shard_path.join(VECTORSET_DIR),
            channel,
        };
        let rsc = RelationConfig {
            path: shard_path.join(RELATIONS_DIR),
            channel,
        };
        let versions = Versions::load(&shard_path.join(VERSION_FILE))?;
        let text_task = || Some(versions.get_texts_reader(&tsc));
        let paragraph_task = || Some(versions.get_paragraphs_reader(&psc));
        let vector_task = || Some(versions.get_vectors_reader(&vsc));
        let relation_task = || Some(versions.get_relations_reader(&rsc));

        let info = info_span!(parent: &span, "text open");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph open");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector open");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation open");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = None;
        let mut paragraph_result = None;
        let mut vector_result = None;
        let mut relation_result = None;
        crossbeam_thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
            s.spawn(|_| relation_result = relation_task());
        })
        .expect("Failed to join threads");
        let fields = text_result.transpose()?;
        let paragraphs = paragraph_result.transpose()?;
        let vectors = vector_result.transpose()?;
        let relations = relation_result.transpose()?;
        let suffixed_root_path = shard_path.to_str().unwrap().to_owned() + "/";

        Ok(ShardReader {
            id,
            metadata,
            root_path: shard_path.to_path_buf(),
            suffixed_root_path,
            text_reader: fields.unwrap(),
            paragraph_reader: paragraphs.unwrap(),
            vector_reader: vectors.unwrap(),
            relation_reader: relations.unwrap(),
            document_service_version: versions.version_texts() as i32,
            paragraph_service_version: versions.version_paragraphs() as i32,
            vector_service_version: versions.version_vectors() as i32,
            relation_service_version: versions.version_relations() as i32,
        })
    }

    /// Return a list of queries to suggest from the original
    /// query. The query with more words will come first. `max_group`
    /// defines the limit of words a query can have.
    #[tracing::instrument(skip_all)]
    fn split_suggest_query(query: String, max_group: usize) -> Vec<String> {
        // Paying the price of allocating the vector to not have to
        // prepend to the partial strings.
        let relevant_words: Vec<_> = query.split(' ').rev().take(max_group).collect();
        let mut prefixes = vec![String::new(); max_group];
        for (index, word) in relevant_words.into_iter().rev().enumerate() {
            // The inner loop is upper-bounded by max_group
            for prefix in prefixes.iter_mut().take(index + 1) {
                if !prefix.is_empty() {
                    prefix.push(' ');
                }
                prefix.push_str(word);
            }
        }
        prefixes
    }

    #[measure(actor = "shard", metric = "suggest")]
    #[tracing::instrument(skip_all)]
    pub fn suggest(&self, request: SuggestRequest) -> NodeResult<SuggestResponse> {
        let span = tracing::Span::current();

        let suggest_paragraphs = request
            .features
            .contains(&(SuggestFeatures::Paragraphs as i32));
        let suggest_entities = request
            .features
            .contains(&(SuggestFeatures::Entities as i32));

        let paragraphs_reader_service = self.paragraph_reader.clone();
        let relations_reader_service = self.relation_reader.clone();
        let prefixes = Self::split_suggest_query(request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);

        let suggest_paragraphs_task = suggest_paragraphs.then(|| {
            let paragraph_task = move || paragraphs_reader_service.suggest(&request);
            let info = info_span!(parent: &span, "paragraph suggest");
            || run_with_telemetry(info, paragraph_task)
        });

        let relation_task = suggest_entities.then(|| {
            let relation_task = move || {
                let requests = prefixes
                    .par_iter()
                    .filter(|prefix| prefix.len() >= MIN_VIABLE_PREFIX_SUGGEST)
                    .cloned()
                    .map(|prefix| RelationSearchRequest {
                        shard_id: String::default(), // REVIEW: really?
                        prefix: Some(RelationPrefixSearchRequest {
                            prefix,
                            ..Default::default()
                        }),
                        ..Default::default()
                    });

                let responses = requests
                    .map(|request| relations_reader_service.search(&request))
                    .collect::<Vec<_>>();

                let entities = responses
                    .into_iter()
                    .flatten() // unwrap errors and continue with successful results
                    .flat_map(|response| response.prefix)
                    .flat_map(|prefix_response| prefix_response.nodes.into_iter())
                    .map(|node| node.value);

                // remove duplicate entities
                let mut seen = HashSet::new();
                let mut ent_result = entities.collect::<Vec<_>>();
                ent_result.retain(|e| seen.insert(e.clone()));

                ent_result
            };

            let info = info_span!(parent: &span, "relations suggest");
            || run_with_telemetry(info, relation_task)
        });

        let mut paragraphs_response = None;
        let mut entities = None;

        crossbeam_thread::scope(|s| {
            if let Some(task) = suggest_paragraphs_task {
                s.spawn(|_| {
                    paragraphs_response = Some(task());
                });
            }
            if let Some(task) = relation_task {
                s.spawn(|_| entities = Some(task()));
            }
        })
        .expect("Failed to join threads");

        let mut response = SuggestResponse::default();

        if let Some(paragraphs_response) = paragraphs_response {
            let paragraphs_response = paragraphs_response?;
            response.query = paragraphs_response.query;
            response.total = paragraphs_response.total;
            response.results = paragraphs_response.results;
            response.ematches = paragraphs_response.ematches;
        };

        if let Some(entities) = entities {
            response.entities = Some(RelatedEntities {
                total: entities.len() as u32,
                entities,
            })
        }

        Ok(response)
    }

    #[measure(actor = "shard", metric = "request/search")]
    #[tracing::instrument(skip_all)]
    pub fn search(&self, search_request: SearchRequest) -> NodeResult<SearchResponse> {
        let query_plan = QueryPlan::from(search_request);

        let search_id = uuid::Uuid::new_v4().to_string();
        let span = tracing::Span::current();
        let pre_filter = query_plan.pre_filter;
        let mut index_queries = query_plan.index_queries;

        // Apply pre-filtering to the query plan
        if let Some(pre_filter) = pre_filter {
            let pre_filtered = self.text_reader.pre_filter(&pre_filter)?;
            index_queries.apply_pre_filter(pre_filtered);
        }

        // Run the rest of the plan
        let text_task = index_queries.texts_request.map(|mut request| {
            request.id = search_id.clone();
            let text_reader_service = self.text_reader.clone();
            let info = info_span!(parent: &span, "text search");
            let task = move || text_reader_service.search(&request);
            || run_with_telemetry(info, task)
        });

        let paragraph_task = index_queries.paragraphs_request.map(|mut request| {
            request.id = search_id.clone();
            let paragraph_reader_service = self.paragraph_reader.clone();
            let info = info_span!(parent: &span, "paragraph search");
            let task = move || paragraph_reader_service.search(&request);
            || run_with_telemetry(info, task)
        });

        let vector_task = index_queries.vectors_request.map(|mut request| {
            request.id = search_id.clone();
            let vector_reader_service = self.vector_reader.clone();
            let info = info_span!(parent: &span, "vector search");
            let task = move || vector_reader_service.search(&request);
            || run_with_telemetry(info, task)
        });

        let relation_task = index_queries.relations_request.map(|request| {
            let relation_reader_service = self.relation_reader.clone();
            let info = info_span!(parent: &span, "relations search");
            let task = move || relation_reader_service.search(&request);
            || run_with_telemetry(info, task)
        });

        let mut rtext = None;
        let mut rparagraph = None;
        let mut rvector = None;
        let mut rrelation = None;

        crossbeam_thread::scope(|s| {
            if let Some(task) = text_task {
                s.spawn(|_| rtext = Some(task()));
            }
            if let Some(task) = paragraph_task {
                s.spawn(|_| rparagraph = Some(task()));
            }
            if let Some(task) = vector_task {
                s.spawn(|_| rvector = Some(task()));
            }
            if let Some(task) = relation_task {
                s.spawn(|_| rrelation = Some(task()));
            }
        })
        .expect("Failed to join threads");

        Ok(SearchResponse {
            document: rtext.transpose()?,
            paragraph: rparagraph.transpose()?,
            vector: rvector.transpose()?,
            relation: rrelation.transpose()?,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_iterator(&self, request: StreamRequest) -> NodeResult<ParagraphIterator> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "paragraph iteration"), || {
            self.paragraph_reader.iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_iterator(&self, request: StreamRequest) -> NodeResult<DocumentIterator> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "field iteration"), || {
            self.text_reader.iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn download_file_iterator(
        &self,
        relative_path: String,
    ) -> NodeResult<ShardFileChunkIterator> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "download file iteration"), || {
            ShardFileChunkIterator::new(self.root_path.join(relative_path), CHUNK_SIZE)
        })
    }

    fn visit_directories(
        &self,
        path: PathBuf,
        to_visit: &mut Vec<PathBuf>,
    ) -> NodeResult<Vec<ShardFile>> {
        let dir = fs::read_dir(path)?;
        let mut files = Vec::new();

        for child in dir {
            let child = child?;
            let child_metadata = child.metadata()?;
            if child_metadata.is_dir() {
                to_visit.push(child.path());
            } else {
                let path = child.path().into_os_string().into_string().unwrap();
                if let Some(path) = path.strip_prefix(&self.suffixed_root_path) {
                    files.push(ShardFile {
                        relative_path: path.to_string(),
                        size: child_metadata.len(),
                    });
                }
            }
        }
        Ok(files)
    }

    #[tracing::instrument(skip_all)]
    pub fn get_shard_files(&self) -> NodeResult<ShardFileList> {
        let mut to_visit = vec![self.root_path.clone()];
        let mut files = Vec::new();

        while let Some(path) = to_visit.pop() {
            let mut files_found = self.visit_directories(path, &mut to_visit)?;
            files.append(&mut files_found);
        }

        Ok(ShardFileList { files })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(
        &self,
        search_request: ParagraphSearchRequest,
    ) -> NodeResult<ParagraphSearchResponse> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
            self.paragraph_reader.search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_search(
        &self,
        search_request: DocumentSearchRequest,
    ) -> NodeResult<DocumentSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
            self.text_reader.search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_search(
        &self,
        search_request: VectorSearchRequest,
    ) -> NodeResult<VectorSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "vector reader search"), || {
            self.vector_reader.search(&search_request)
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_search(
        &self,
        search_request: RelationSearchRequest,
    ) -> NodeResult<RelationSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "relation reader search"), || {
            self.relation_reader.search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_count(&self) -> NodeResult<usize> {
        self.paragraph_reader.count()
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_count(&self, vector_set: &str) -> NodeResult<usize> {
        self.vector_reader.count(vector_set)
    }

    #[tracing::instrument(skip_all)]
    pub fn text_count(&self) -> NodeResult<usize> {
        self.text_reader.count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_split() {
        let query = "Some search with multiple words".to_string();

        let expected = vec!["with multiple words", "multiple words", "words"];
        let got = ShardReader::split_suggest_query(query.clone(), 3);
        assert_eq!(expected, got);

        let expected = vec!["multiple words", "words"];
        let got = ShardReader::split_suggest_query(query, 2);
        assert_eq!(expected, got);
    }
}
