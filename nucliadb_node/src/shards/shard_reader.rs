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

use super::indexes::ShardIndexes;
use super::indexes::DEFAULT_VECTORS_INDEX_NAME;
use super::metadata::ShardMetadata;
use super::versioning::Versions;
use crate::disk_structure::*;
use crate::telemetry::run_with_telemetry;
use crossbeam_utils::thread as crossbeam_thread;
use nucliadb_core::paragraphs::*;
use nucliadb_core::prelude::*;
use nucliadb_core::protos;
use nucliadb_core::protos::shard_created::{DocumentService, ParagraphService, RelationService, VectorService};
use nucliadb_core::protos::{
    DocumentSearchRequest, DocumentSearchResponse, EdgeList, ParagraphSearchRequest, ParagraphSearchResponse,
    RelationPrefixSearchRequest, RelationSearchRequest, RelationSearchResponse, SearchRequest, SearchResponse, Shard,
    ShardFile, ShardFileChunk, ShardFileList, StreamRequest, SuggestFeatures, SuggestRequest, SuggestResponse,
    VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_core::query_language::BooleanExpression;
use nucliadb_core::query_language::BooleanOperation;
use nucliadb_core::query_language::Operator;
use nucliadb_core::query_planner;
use nucliadb_core::query_planner::PreFilterRequest;
use nucliadb_core::relations::*;
use nucliadb_core::texts::*;
use nucliadb_core::thread::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::*;
use nucliadb_procs::measure;
use nucliadb_protos::nodereader::{RelationNodeFilter, RelationPrefixSearchResponse};
use nucliadb_protos::utils::relation_node::NodeType;
use nucliadb_relations2::reader::HashedRelationNode;
use std::collections::HashMap;
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufReader, Read};
use std::path::{Path, PathBuf};
use std::sync::RwLock;

const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;
const MIN_VIABLE_PREFIX_SUGGEST: usize = 1;
const CHUNK_SIZE: usize = 65535;

fn open_vectors_reader(version: u32, path: &Path) -> NodeResult<VectorsReaderPointer> {
    match version {
        1 => nucliadb_vectors::service::VectorReaderService::open(path).map(|i| Box::new(i) as VectorsReaderPointer),
        2 => nucliadb_vectors::service::VectorReaderService::open(path).map(|i| Box::new(i) as VectorsReaderPointer),
        v => Err(node_error!("Invalid vectors version {v}")),
    }
}
fn open_paragraphs_reader(version: u32, path: &Path) -> NodeResult<ParagraphsReaderPointer> {
    match version {
        3 => nucliadb_paragraphs3::reader::ParagraphReaderService::open(path)
            .map(|i| Box::new(i) as ParagraphsReaderPointer),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}

fn open_texts_reader(version: u32, path: &Path) -> NodeResult<TextsReaderPointer> {
    match version {
        2 => nucliadb_texts2::reader::TextReaderService::open(path).map(|i| Box::new(i) as TextsReaderPointer),
        3 => nucliadb_texts3::reader::TextReaderService::open(path).map(|i| Box::new(i) as TextsReaderPointer),
        v => Err(node_error!("Invalid text reader version {v}")),
    }
}

fn open_relations_reader(version: u32, path: &Path) -> NodeResult<RelationsReaderPointer> {
    match version {
        2 => nucliadb_relations2::reader::RelationsReaderService::open(path)
            .map(|i| Box::new(i) as RelationsReaderPointer),
        v => Err(node_error!("Invalid relations version {v}")),
    }
}

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
    text_reader: RwLock<TextsReaderPointer>,
    paragraph_reader: RwLock<ParagraphsReaderPointer>,
    // vector index searches are not intended to run in parallel, so we only
    // need a lock for all of them
    vector_readers: RwLock<HashMap<String, VectorsReaderPointer>>,
    relation_reader: RwLock<RelationsReaderPointer>,
    versions: Versions,
}

impl ShardReader {
    #[tracing::instrument(skip_all)]
    pub fn text_version(&self) -> DocumentService {
        match self.versions.texts {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            2 => DocumentService::DocumentV2,
            3 => DocumentService::DocumentV3,
            i => panic!("Unknown document version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_version(&self) -> ParagraphService {
        match self.versions.paragraphs {
            0 => ParagraphService::ParagraphV0,
            1 => ParagraphService::ParagraphV1,
            2 => ParagraphService::ParagraphV2,
            3 => ParagraphService::ParagraphV3,
            i => panic!("Unknown paragraph version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_version(&self) -> VectorService {
        match self.versions.vectors {
            0 => VectorService::VectorV0,
            1 => VectorService::VectorV1,
            i => panic!("Unknown vector version {i}"),
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn relation_version(&self) -> RelationService {
        match self.versions.relations {
            0 => RelationService::RelationV0,
            1 => RelationService::RelationV1,
            2 => RelationService::RelationV2,
            i => panic!("Unknown relation version {i}"),
        }
    }

    #[measure(actor = "shard", metric = "get_info")]
    #[tracing::instrument(skip_all)]
    pub fn get_info(&self) -> NodeResult<Shard> {
        let span = tracing::Span::current();

        let info = info_span!(parent: &span, "text count");
        let text_task = || run_with_telemetry(info, || read_rw_lock(&self.text_reader).count());
        let info = info_span!(parent: &span, "paragraph count");
        let paragraph_task = || run_with_telemetry(info, || read_rw_lock(&self.paragraph_reader).count());
        let info = info_span!(parent: &span, "vector count");
        let vector_task = || {
            run_with_telemetry(info, || {
                let vector_readers = read_rw_lock(&self.vector_readers);
                if let Some(reader) = vector_readers.get(DEFAULT_VECTORS_INDEX_NAME) {
                    return reader.count();
                }

                let mut count = 0;
                for reader in vector_readers.values() {
                    count += reader.count()?;
                }
                Ok(count)
            })
        };

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
            metadata: Some(protos::ShardMetadata {
                kbid: self.metadata.kbid(),
                release_channel: 0,
            }),
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
        read_rw_lock(&self.text_reader).stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_paragraphs_keys(&self) -> NodeResult<Vec<String>> {
        read_rw_lock(&self.paragraph_reader).stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_vectors_keys(&self, vectorset_id: &str) -> NodeResult<Vec<String>> {
        if let Some(reader) = read_rw_lock(&self.vector_readers).get(vectorset_id) {
            reader.stored_ids()
        } else {
            Err(node_error!("Vectorset {vectorset_id} does not exist"))
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn get_relations_keys(&self) -> NodeResult<Vec<String>> {
        read_rw_lock(&self.relation_reader).stored_ids()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_relations_edges(&self) -> NodeResult<EdgeList> {
        read_rw_lock(&self.relation_reader).get_edges()
    }

    #[measure(actor = "shard", metric = "new")]
    #[tracing::instrument(skip_all)]
    pub fn new(id: String, shard_path: &Path) -> NodeResult<ShardReader> {
        let span = tracing::Span::current();

        let metadata = ShardMetadata::open(shard_path.to_path_buf())?;
        let indexes = ShardIndexes::load(shard_path).unwrap_or_else(|_| ShardIndexes::new(shard_path));

        let versions = Versions::load(&shard_path.join(VERSION_FILE))?;

        let text_task = || Some(open_texts_reader(versions.texts, &indexes.texts_path()));
        let info = info_span!(parent: &span, "text open");
        let text_task = || run_with_telemetry(info, text_task);

        let paragraph_task = || Some(open_paragraphs_reader(versions.paragraphs, &indexes.paragraphs_path()));
        let info = info_span!(parent: &span, "paragraph open");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);

        let mut vector_tasks = vec![];
        for (name, path) in indexes.iter_vectors_indexes() {
            vector_tasks.push(|| {
                run_with_telemetry(info_span!(parent: &span, "vector open"), move || {
                    Some((name, open_vectors_reader(versions.vectors, &path)))
                })
            });
        }

        let relation_task = || Some(open_relations_reader(versions.relations, &indexes.relations_path()));
        let info = info_span!(parent: &span, "relation open");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = None;
        let mut paragraph_result = None;
        let mut vector_results = Vec::with_capacity(vector_tasks.len());
        for _ in 0..vector_tasks.len() {
            vector_results.push(None);
        }
        let mut relation_result = None;
        crossbeam_thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            for (vector_task, vector_result) in vector_tasks.into_iter().zip(vector_results.iter_mut()) {
                s.spawn(|_| *vector_result = vector_task());
            }
            s.spawn(|_| relation_result = relation_task());
        })
        .expect("Failed to join threads");
        let fields = text_result.transpose()?;
        let paragraphs = paragraph_result.transpose()?;
        let mut vectors = HashMap::with_capacity(vector_results.len());
        for result in vector_results {
            let (name, vector_writer) = result.unwrap();
            vectors.insert(name, vector_writer?);
        }
        let relations = relation_result.transpose()?;
        let suffixed_root_path = shard_path.to_str().unwrap().to_owned() + "/";

        Ok(ShardReader {
            id,
            metadata,
            suffixed_root_path,
            root_path: shard_path.to_path_buf(),
            text_reader: RwLock::new(fields.unwrap()),
            paragraph_reader: RwLock::new(paragraphs.unwrap()),
            vector_readers: RwLock::new(vectors),
            relation_reader: RwLock::new(relations.unwrap()),
            versions,
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
    pub fn suggest(&self, mut request: SuggestRequest) -> NodeResult<SuggestResponse> {
        let span = tracing::Span::current();

        let mut suggest_paragraphs = request.features.contains(&(SuggestFeatures::Paragraphs as i32));
        let suggest_entities = request.features.contains(&(SuggestFeatures::Entities as i32));
        let prefixes = Self::split_suggest_query(request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);

        // Prefilter to apply field label filters
        if let Some(filter) = &mut request.filter {
            if !filter.field_labels.is_empty() && suggest_paragraphs {
                let labels = std::mem::take(&mut filter.field_labels);
                let operands = labels.into_iter().map(BooleanExpression::Literal).collect();
                let op = BooleanOperation {
                    operator: Operator::And,
                    operands,
                };
                let prefilter = PreFilterRequest {
                    timestamp_filters: vec![],
                    security: None,
                    labels_formula: Some(BooleanExpression::Operation(op)),
                    keywords_formula: None,
                };

                let prefiltered = read_rw_lock(&self.text_reader).prefilter(&prefilter)?;

                // Apply prefilter to paragraphs query
                match prefiltered.valid_fields {
                    query_planner::ValidFieldCollector::All => {}
                    query_planner::ValidFieldCollector::Some(keys) => {
                        request.key_filters = keys.iter().map(|v| format!("{}{}", v.resource_id, v.field_id)).collect()
                    }
                    query_planner::ValidFieldCollector::None => suggest_paragraphs = false,
                }
            }
        }

        let suggest_paragraphs_task = suggest_paragraphs.then(|| {
            let paragraph_task = move || read_rw_lock(&self.paragraph_reader).suggest(&request);
            let info = info_span!(parent: &span, "paragraph suggest");
            || run_with_telemetry(info, paragraph_task)
        });

        let relation_task = suggest_entities.then(|| {
            let relation_task = move || {
                let requests =
                    prefixes.par_iter().filter(|prefix| prefix.len() >= MIN_VIABLE_PREFIX_SUGGEST).cloned().map(
                        |prefix| RelationSearchRequest {
                            prefix: Some(RelationPrefixSearchRequest {
                                prefix,
                                node_filters: vec![RelationNodeFilter {
                                    node_type: NodeType::Entity.into(),
                                    ..Default::default()
                                }],
                            }),
                            ..Default::default()
                        },
                    );

                let responses: Vec<_> =
                    requests.map(|request| read_rw_lock(&self.relation_reader).search(&request)).collect();

                let entities = responses
                    .into_iter()
                    .flatten() // unwrap errors and continue with successful results
                    .flat_map(|response| response.prefix)
                    .flat_map(|prefix_response| prefix_response.nodes.into_iter());

                // remove duplicate entities
                let mut seen: HashSet<HashedRelationNode> = HashSet::new();
                let mut ent_result = entities.collect::<Vec<_>>();
                ent_result.retain(|e| seen.insert(e.clone().into()));

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
            response.entity_results = Some(RelationPrefixSearchResponse {
                nodes: entities,
            });
        }

        Ok(response)
    }

    #[measure(actor = "shard", metric = "request/search")]
    #[tracing::instrument(skip_all)]
    pub fn search(&self, search_request: SearchRequest) -> NodeResult<SearchResponse> {
        let query_plan = query_planner::build_query_plan(self.versions.paragraphs, search_request)?;
        let search_id = uuid::Uuid::new_v4().to_string();
        let span = tracing::Span::current();
        let mut index_queries = query_plan.index_queries;

        // Apply pre-filtering to the query plan
        if let Some(prefilter) = &query_plan.prefilter {
            let prefiltered = read_rw_lock(&self.text_reader).prefilter(prefilter)?;
            index_queries.apply_prefilter(prefiltered);
        }

        // Run the rest of the plan
        let text_task = index_queries.texts_request.map(|mut request| {
            request.id = search_id.clone();
            let info = info_span!(parent: &span, "text search");
            let task = move || read_rw_lock(&self.text_reader).search(&request);
            || run_with_telemetry(info, task)
        });

        let paragraph_task = index_queries.paragraphs_request.map(|mut request| {
            request.id = search_id.clone();
            let paragraphs_context = &index_queries.paragraphs_context;
            let info = info_span!(parent: &span, "paragraph search");
            let task = move || read_rw_lock(&self.paragraph_reader).search(&request, paragraphs_context);
            || run_with_telemetry(info, task)
        });

        let vector_task = index_queries.vectors_request.map(|mut request| {
            request.id = search_id.clone();
            let vectors_context = &index_queries.vectors_context;
            let task = move || self.vectors_index_search(&request, vectors_context);
            || run_with_telemetry(info_span!(parent: &span, "vector search"), task)
        });

        let relation_task = index_queries.relations_request.map(|request| {
            let info = info_span!(parent: &span, "relations search");
            let task = move || read_rw_lock(&self.relation_reader).search(&request);
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
            read_rw_lock(&self.paragraph_reader).iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_iterator(&self, request: StreamRequest) -> NodeResult<DocumentIterator> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "field iteration"), || {
            read_rw_lock(&self.text_reader).iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn download_file_iterator(&self, relative_path: String) -> NodeResult<ShardFileChunkIterator> {
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "download file iteration"), || {
            ShardFileChunkIterator::new(self.root_path.join(relative_path), CHUNK_SIZE)
        })
    }

    fn visit_directories(&self, path: PathBuf, to_visit: &mut Vec<PathBuf>) -> NodeResult<Vec<ShardFile>> {
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

        Ok(ShardFileList {
            files,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(&self, search_request: ParagraphSearchRequest) -> NodeResult<ParagraphSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
            read_rw_lock(&self.paragraph_reader).search(&search_request, &ParagraphsContext::default())
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_search(&self, search_request: DocumentSearchRequest) -> NodeResult<DocumentSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
            read_rw_lock(&self.text_reader).search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_search(&self, search_request: VectorSearchRequest) -> NodeResult<VectorSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "vector reader search"), || {
            self.vectors_index_search(&search_request, &VectorsContext::default())
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_search(&self, search_request: RelationSearchRequest) -> NodeResult<RelationSearchResponse> {
        let span = tracing::Span::current();

        run_with_telemetry(info_span!(parent: &span, "relation reader search"), || {
            read_rw_lock(&self.relation_reader).search(&search_request)
        })
    }

    pub fn update(&self) -> NodeResult<()> {
        let shard_path = self.metadata.shard_path();
        // TODO: while we don't have all shards migrated, we still have to
        // unwrap with a default
        let indexes = ShardIndexes::load(&shard_path).unwrap_or_else(|_| ShardIndexes::new(&shard_path));

        let mut updated_indexes = HashMap::with_capacity(indexes.count_vectors_indexes());
        let mut keep_indexes = vec![];
        let vector_indexes = read_rw_lock(&self.vector_readers);
        for (vectorset, path) in indexes.iter_vectors_indexes() {
            if let Some(existing_index) = vector_indexes.get(&vectorset) {
                if let Ok(false) = existing_index.needs_update() {
                    // Index already loaded and does not need update, skip
                    keep_indexes.push(vectorset);
                    continue;
                }
            }
            let new_reader = open_vectors_reader(self.versions.vectors, &path)?;
            updated_indexes.insert(vectorset, new_reader);
        }
        drop(vector_indexes);

        let mut vector_indexes = write_rw_lock(&self.vector_readers);
        for keep in keep_indexes {
            if let Some(index) = vector_indexes.remove(&keep) {
                updated_indexes.insert(keep, index);
            }
        }
        *vector_indexes = updated_indexes;
        Ok(())
    }

    fn vectors_index_search(
        &self,
        request: &VectorSearchRequest,
        context: &VectorsContext,
    ) -> NodeResult<VectorSearchResponse> {
        let vectorset = &request.vector_set;
        if vectorset.is_empty() {
            let vector_readers = read_rw_lock(&self.vector_readers);
            if let Some(reader) = vector_readers.get(DEFAULT_VECTORS_INDEX_NAME) {
                reader.search(request, context)
            } else if vector_readers.len() == 1 {
                // no default vectorset but only one exist, consider it the
                // default
                let reader = vector_readers.values().next().unwrap();
                reader.search(request, context)
            } else {
                Err(node_error!("Query without vectorset but shard has multiple vector indexes"))
            }
        } else {
            let vector_readers = read_rw_lock(&self.vector_readers);
            let reader = vector_readers.get(vectorset);
            if let Some(reader) = reader {
                reader.search(request, context)
            } else if vector_readers.len() == 1 && vector_readers.contains_key(DEFAULT_VECTORS_INDEX_NAME) {
                // Only one vectorset with default name, use it!
                // We can remove this once all vectorsets are named (there are no default vectorsets)
                vector_readers.get(DEFAULT_VECTORS_INDEX_NAME).unwrap().search(request, context)
            } else {
                Err(node_error!("Vectorset '{vectorset}' not found"))
            }
        }
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
