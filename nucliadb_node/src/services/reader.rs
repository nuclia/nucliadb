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

use std::path::Path;
use std::sync::RwLock;
use std::time::SystemTime;

use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_core::protos::{
    DocumentSearchRequest, DocumentSearchResponse, EdgeList, GetShardRequest,
    ParagraphSearchRequest, ParagraphSearchResponse, RelatedEntities, RelationPrefixSearchRequest,
    RelationSearchRequest, RelationSearchResponse, SearchRequest, SearchResponse, StreamRequest,
    SuggestRequest, SuggestResponse, TypeList, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_core::thread::{self, *};
use nucliadb_core::tracing::{self, *};

use super::shard_disk_structure::*;
use super::versions::Versions;
use crate::stats::StatsData;
use crate::telemetry::run_with_telemetry;

const RELOAD_PERIOD: u128 = 5000;
const FIXED_VECTORS_RESULTS: usize = 10;
const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;

#[derive(Debug)]
pub struct ShardReaderService {
    pub id: String,
    creation_time: RwLock<SystemTime>,
    text_reader: TextsReaderPointer,
    paragraph_reader: ParagraphsReaderPointer,
    vector_reader: VectorsReaderPointer,
    relation_reader: RelationsReaderPointer,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardReaderService {
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
            i => panic!("Unknown relation version {i}"),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn get_info(&self, request: &GetShardRequest) -> NodeResult<StatsData> {
        self.reload_policy(true);
        let resources = self.text_reader.count()?;
        let paragraphs = self.paragraph_reader.count()?;
        let sentences = self.vector_reader.count(&request.vectorset)?;
        let relations = self.relation_reader.count()?;
        Ok(StatsData {
            resources,
            paragraphs,
            sentences,
            relations,
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn get_text_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.text_reader.stored_ids()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_paragraphs_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.paragraph_reader.stored_ids()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_vectors_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.vector_reader.stored_ids()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_relations_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.relation_reader.stored_ids()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_relations_edges(&self) -> NodeResult<EdgeList> {
        self.reload_policy(true);
        self.relation_reader.get_edges()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_relations_types(&self) -> NodeResult<TypeList> {
        self.reload_policy(true);
        self.relation_reader.get_node_types()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_resources(&self) -> NodeResult<usize> {
        self.text_reader.count()
    }

    #[tracing::instrument(skip_all)]
    pub fn new(id: String, shard_path: &Path) -> NodeResult<ShardReaderService> {
        let tsc = TextConfig {
            path: shard_path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: shard_path.join(PARAGRAPHS_DIR),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: shard_path.join(VECTORS_DIR),
            vectorset: shard_path.join(VECTORSET_DIR),
        };
        let rsc = RelationConfig {
            path: shard_path.join(RELATIONS_DIR),
        };
        let versions = Versions::load(&shard_path.join(VERSION_FILE))?;
        let text_task = || Some(versions.get_texts_reader(&tsc));
        let paragraph_task = || Some(versions.get_paragraphs_reader(&psc));
        let vector_task = || Some(versions.get_vectors_reader(&vsc));
        let relation_task = || Some(versions.get_relations_reader(&rsc));

        let span = tracing::Span::current();
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
        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
            s.spawn(|_| relation_result = relation_task());
        });

        let fields = text_result.transpose()?;
        let paragraphs = paragraph_result.transpose()?;
        let vectors = vector_result.transpose()?;
        let relations = relation_result.transpose()?;

        Ok(ShardReaderService {
            id,
            text_reader: fields.unwrap(),
            paragraph_reader: paragraphs.unwrap(),
            vector_reader: vectors.unwrap(),
            relation_reader: relations.unwrap(),
            creation_time: RwLock::new(SystemTime::now()),
            document_service_version: versions.version_texts() as i32,
            paragraph_service_version: versions.version_paragraphs() as i32,
            vector_service_version: versions.version_vectors() as i32,
            relation_service_version: versions.version_relations() as i32,
        })
    }

    /// Stop the service
    #[tracing::instrument(skip_all)]
    pub fn stop(&self) {
        info!("Stopping shard {}...", { &self.id });
        let fields = self.text_reader.clone();
        let paragraphs = self.paragraph_reader.clone();
        let vectors = self.vector_reader.clone();
        let relations = self.relation_reader.clone();

        let text_task = move || fields.stop();
        let paragraph_task = move || paragraphs.stop();
        let vector_task = move || vectors.stop();
        let relation_task = move || relations.stop();

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text stop");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph stop");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector stop");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation stop");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = Ok(());
        let mut paragraph_result = Ok(());
        let mut vector_result = Ok(());
        let mut relation_result = Ok(());
        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
            s.spawn(|_| relation_result = relation_task());
        });

        if let Err(e) = text_result {
            error!("Error stopping the Field reader service: {}", e);
        }
        if let Err(e) = paragraph_result {
            error!("Error stopping the Paragraph reader service: {}", e);
        }
        if let Err(e) = vector_result {
            error!("Error stopping the Vector reader service: {}", e);
        }
        if let Err(e) = relation_result {
            error!("Error stopping the Relation reader service: {}", e);
        }
        info!("Shard stopped {}...", { &self.id });
    }

    /// Return a list of queries to suggest from the original
    /// query. The query with more words will come first. `max_group`
    /// defines the limit of words a query can have.
    #[tracing::instrument(skip_all)]
    fn split_suggest_query(query: String, max_group: usize) -> Vec<String> {
        let words = query.split(' ');
        let mut i = 0;
        let mut prefixes = vec![];
        let mut prefix = String::new();

        for word in words.rev() {
            if prefix.is_empty() {
                prefix = word.to_string();
            } else {
                prefix = format!("{word} {prefix}");
            }
            prefixes.push(prefix.clone());

            i += 1;
            if i == max_group {
                break;
            }
        }

        prefixes.into_iter().rev().collect()
    }

    #[tracing::instrument(skip_all)]
    pub fn suggest(&self, request: SuggestRequest) -> NodeResult<SuggestResponse> {
        // Search for entities related to the query.

        let relations_reader_service = self.relation_reader.clone();
        let paragraph_reader_service = self.paragraph_reader.clone();

        let prefixes = Self::split_suggest_query(request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);
        let relations = prefixes.par_iter().map(|prefix| {
            let request = RelationSearchRequest {
                shard_id: String::default(),
                prefix: Some(RelationPrefixSearchRequest {
                    prefix: prefix.clone(),
                }),
                ..Default::default()
            };
            relations_reader_service.search(&request)
        });

        let relation_task = move || relations.collect::<Vec<_>>();
        let paragraph_task = move || paragraph_reader_service.suggest(&request);
        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "relations suggest");
        let relation_task = || run_with_telemetry(info, relation_task);
        let info = info_span!(parent: &span, "paragraph suggest");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);

        let tasks = thread::join(paragraph_task, relation_task);
        let rparagraph = tasks.0.unwrap();
        let entities = tasks
            .1
            .into_iter()
            .flat_map(|relation| {
                relation
                    .unwrap()
                    .prefix
                    .expect("Prefix search request must return a prefix response")
                    .nodes
                    .iter()
                    .map(|relation_node| relation_node.value.clone())
                    .collect::<Vec<String>>()
            })
            .collect::<Vec<String>>();

        Ok(SuggestResponse {
            query: rparagraph.query,
            total: rparagraph.total,
            results: rparagraph.results,
            ematches: rparagraph.ematches,
            entities: Some(RelatedEntities {
                total: entities.len() as u32,
                entities,
            }),
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn search(&self, search_request: SearchRequest) -> NodeResult<SearchResponse> {
        self.reload_policy(search_request.reload);
        let skip_paragraphs = !search_request.paragraph;
        let skip_fields = !search_request.document;
        let skip_vectors = search_request.result_per_page == 0 || search_request.vector.is_empty();

        let field_request = DocumentSearchRequest {
            id: "".to_string(),
            body: search_request.body.clone(),
            fields: search_request.fields.clone(),
            filter: search_request.filter.clone(),
            order: search_request.order.clone(),
            faceted: search_request.faceted.clone(),
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            timestamps: search_request.timestamps.clone(),
            reload: search_request.reload,
            only_faceted: search_request.only_faceted,
            advanced_query: search_request.advanced_query.clone(),
            with_status: search_request.with_status,
        };
        let text_reader_service = self.text_reader.clone();
        let text_task = move || {
            if !skip_fields {
                Some(text_reader_service.search(&field_request))
            } else {
                None
            }
        };

        let paragraph_request = ParagraphSearchRequest {
            id: "".to_string(),
            uuid: "".to_string(),
            with_duplicates: search_request.with_duplicates,
            body: search_request.body.clone(),
            fields: search_request.fields.clone(),
            filter: search_request.filter.clone(),
            order: search_request.order.clone(),
            faceted: search_request.faceted.clone(),
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            timestamps: search_request.timestamps.clone(),
            reload: search_request.reload,
            only_faceted: search_request.only_faceted,
            advanced_query: search_request.advanced_query.clone(),
        };
        let paragraph_reader_service = self.paragraph_reader.clone();
        let paragraph_task = move || {
            if !skip_paragraphs {
                Some(paragraph_reader_service.search(&paragraph_request))
            } else {
                None
            }
        };

        let vector_request = VectorSearchRequest {
            id: "".to_string(),
            vector_set: search_request.vectorset.clone(),
            vector: search_request.vector.clone(),
            reload: search_request.reload,
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            with_duplicates: true,
            tags: search_request
                .filter
                .iter()
                .flat_map(|f| f.tags.iter().cloned())
                .chain(search_request.fields.iter().cloned())
                .collect(),
        };
        let vector_reader_service = self.vector_reader.clone();
        let vector_task = move || {
            if !skip_vectors {
                Some(vector_reader_service.search(&vector_request))
            } else {
                None
            }
        };

        let relation_reader_service = self.relation_reader.clone();
        let relation_task = move || {
            search_request
                .relations
                .map(|relation_request| relation_reader_service.search(&relation_request))
        };

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text search");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph search");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector search");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relations search");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut rtext = None;
        let mut rparagraph = None;
        let mut rvector = None;
        let mut rrelation = None;

        thread::scope(|s| {
            s.spawn(|_| rtext = text_task());
            s.spawn(|_| rparagraph = paragraph_task());
            s.spawn(|_| rvector = vector_task());
            s.spawn(|_| rrelation = relation_task());
        });
        Ok(SearchResponse {
            document: rtext.transpose()?,
            paragraph: rparagraph.transpose()?,
            vector: rvector.transpose()?,
            relation: rrelation.transpose()?,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(
        &self,
        search_request: ParagraphSearchRequest,
    ) -> NodeResult<ParagraphSearchResponse> {
        self.reload_policy(search_request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
            self.paragraph_reader.search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_iterator(&self, request: StreamRequest) -> NodeResult<ParagraphIterator> {
        self.reload_policy(request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "paragraph iteration"), || {
            self.paragraph_reader.iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_iterator(&self, request: StreamRequest) -> NodeResult<DocumentIterator> {
        self.reload_policy(request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "field iteration"), || {
            self.text_reader.iterator(&request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_search(
        &self,
        search_request: DocumentSearchRequest,
    ) -> NodeResult<DocumentSearchResponse> {
        self.reload_policy(search_request.reload);
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
        self.reload_policy(search_request.reload);
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
        self.reload_policy(search_request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "relation reader search"), || {
            self.relation_reader.search(&search_request)
        })
    }

    fn reload_policy(&self, trigger: bool) {
        let elapsed = self
            .creation_time
            .read()
            .unwrap()
            .elapsed()
            .unwrap()
            .as_millis();
        if trigger || elapsed >= RELOAD_PERIOD {
            let mut creation_time = self.creation_time.write().unwrap();
            *creation_time = SystemTime::now();
            self.vector_reader.reload()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_suggest_split() {
        let query = "Some search with multiple words".to_string();

        assert_eq!(
            ShardReaderService::split_suggest_query(query.clone(), 3),
            vec!["with multiple words", "multiple words", "words"]
        );
        assert_eq!(
            ShardReaderService::split_suggest_query(query, 2),
            vec!["multiple words", "words"]
        );
    }
}
