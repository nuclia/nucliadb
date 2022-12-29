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

use nucliadb_protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_protos::{
    relation_node, DocumentSearchRequest, DocumentSearchResponse, EdgeList, GetShardRequest,
    ParagraphSearchRequest, ParagraphSearchResponse, RelatedEntities, RelationSearchRequest,
    RelationSearchResponse, SearchRequest, SearchResponse, SuggestRequest, SuggestResponse,
    TypeList, VectorSearchRequest, VectorSearchResponse,
};
use nucliadb_services::*;
use rayon::prelude::*;
use tracing::*;

use crate::services::config::ShardConfig;
use crate::stats::StatsData;
use crate::telemetry::run_with_telemetry;

const RELOAD_PERIOD: u128 = 5000;
const FIXED_VECTORS_RESULTS: usize = 10;
const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;

#[derive(Debug)]
pub struct ShardReaderService {
    pub id: String,
    creation_time: RwLock<SystemTime>,
    field_reader: fields::RFields,
    paragraph_reader: paragraphs::RParagraphs,
    vector_reader: vectors::RVectors,
    relation_reader: relations::RRelations,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardReaderService {
    #[tracing::instrument(skip_all)]
    pub fn document_version(&self) -> DocumentService {
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
    pub fn get_info(&self, request: &GetShardRequest) -> InternalResult<StatsData> {
        self.reload_policy(true);
        let resources = self.field_reader.count()?;
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
    pub fn get_field_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.field_reader.stored_ids()
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
    pub fn get_relations_edges(&self) -> InternalResult<EdgeList> {
        self.reload_policy(true);
        self.relation_reader.get_edges()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_relations_types(&self) -> InternalResult<TypeList> {
        self.reload_policy(true);
        self.relation_reader.get_node_types()
    }
    #[tracing::instrument(skip_all)]
    pub fn get_resources(&self) -> InternalResult<usize> {
        self.field_reader.count()
    }

    #[tracing::instrument(skip_all)]
    pub fn open(id: String, shard_path: &Path) -> InternalResult<ShardReaderService> {
        let fsc = FieldConfig {
            path: shard_path.join("text"),
        };

        let psc = ParagraphConfig {
            path: shard_path.join("paragraph"),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: shard_path.join("vectors"),
            vectorset: shard_path.join("vectorset"),
        };
        let rsc = RelationConfig {
            path: shard_path.join("relations"),
        };

        let config = ShardConfig::new(shard_path);
        let text_task = move || Some(fields::open_reader(&fsc, config.version_fields));
        let paragraph_task = move || Some(paragraphs::open_reader(&psc, config.version_paragraphs));
        let vector_task = move || Some(vectors::open_reader(&vsc, config.version_vectors));
        let relation_task = move || Some(relations::open_reader(&rsc, config.version_relations));

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
        rayon::scope(|s| {
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
            field_reader: fields.unwrap(),
            paragraph_reader: paragraphs.unwrap(),
            vector_reader: vectors.unwrap(),
            relation_reader: relations.unwrap(),
            creation_time: RwLock::new(SystemTime::now()),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn create(id: String, shard_path: &Path) -> InternalResult<ShardReaderService> {
        let fsc = FieldConfig {
            path: shard_path.join("text"),
        };

        let psc = ParagraphConfig {
            path: shard_path.join("paragraph"),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: shard_path.join("vectors"),
            vectorset: shard_path.join("vectorset"),
        };
        let rsc = RelationConfig {
            path: shard_path.join("relations"),
        };

        let config = ShardConfig::new(shard_path);
        let text_task = move || Some(fields::create_reader(&fsc, config.version_fields));
        let paragraph_task =
            move || Some(paragraphs::create_reader(&psc, config.version_paragraphs));
        let vector_task = move || Some(vectors::create_reader(&vsc, config.version_vectors));
        let relation_task = move || Some(relations::create_reader(&rsc, config.version_relations));

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text create");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph create");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector create");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation create");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = None;
        let mut paragraph_result = None;
        let mut vector_result = None;
        let mut relation_result = None;
        rayon::scope(|s| {
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
            field_reader: fields.unwrap(),
            paragraph_reader: paragraphs.unwrap(),
            vector_reader: vectors.unwrap(),
            relation_reader: relations.unwrap(),
            creation_time: RwLock::new(SystemTime::now()),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    /// Stop the service

    #[tracing::instrument(skip_all)]
    pub fn stop(&self) {
        info!("Stopping shard {}...", { &self.id });
        let fields = self.field_reader.clone();
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
        rayon::scope(|s| {
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
    pub fn suggest(&self, request: SuggestRequest) -> InternalResult<SuggestResponse> {
        // Search for entities related to the query.

        let relations_reader_service = self.relation_reader.clone();
        let paragraph_reader_service = self.paragraph_reader.clone();

        let prefixes = Self::split_suggest_query(request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);
        let relations = prefixes.par_iter().map(|prefix| {
            let filter = RelationFilter {
                ntype: relation_node::NodeType::Entity as i32,
                subtype: "".to_string(),
            };
            let request = RelationSearchRequest {
                id: String::default(),
                prefix: prefix.clone(),
                type_filters: vec![filter],
                depth: 10,
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

        let tasks = rayon::join(paragraph_task, relation_task);
        let rparagraph = tasks.0.unwrap();
        let entities = tasks
            .1
            .into_iter()
            .flat_map(|relation| {
                relation
                    .unwrap()
                    .neighbours
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
    pub fn search(&self, search_request: SearchRequest) -> InternalResult<SearchResponse> {
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

        let field_reader_service = self.field_reader.clone();
        let text_task = move || {
            if !skip_fields {
                Some(field_reader_service.search(&field_request))
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

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text search");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph search");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector search");
        let vector_task = || run_with_telemetry(info, vector_task);

        let mut rtext = None;
        let mut rparagraph = None;
        let mut rvector = None;
        rayon::scope(|s| {
            s.spawn(|_| rtext = text_task());
            s.spawn(|_| rparagraph = paragraph_task());
            s.spawn(|_| rvector = vector_task());
        });
        Ok(SearchResponse {
            document: rtext.transpose()?,
            paragraph: rparagraph.transpose()?,
            vector: rvector.transpose()?,
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_search(
        &self,
        search_request: ParagraphSearchRequest,
    ) -> InternalResult<ParagraphSearchResponse> {
        self.reload_policy(search_request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
            self.paragraph_reader.search(&search_request)
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn document_search(
        &self,
        search_request: DocumentSearchRequest,
    ) -> InternalResult<DocumentSearchResponse> {
        self.reload_policy(search_request.reload);
        let span = tracing::Span::current();
        run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
            self.field_reader.search(&search_request)
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_search(
        &self,
        search_request: VectorSearchRequest,
    ) -> InternalResult<VectorSearchResponse> {
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
    ) -> InternalResult<RelationSearchResponse> {
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
