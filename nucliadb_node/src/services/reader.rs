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

use futures::future::try_join_all;
use nucliadb_protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_protos::{
    relation_node, DocumentSearchRequest, DocumentSearchResponse, EdgeList, ParagraphSearchRequest,
    ParagraphSearchResponse, RelatedEntities, RelationSearchRequest, RelationSearchResponse,
    SearchRequest, SearchResponse, SuggestRequest, SuggestResponse, TypeList, VectorSearchRequest,
    VectorSearchResponse,
};
use nucliadb_services::*;
use tokio::{task, try_join};
use tracing::*;

use crate::config::Configuration;
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
    field_reader_service: fields::RFields,
    paragraph_reader_service: paragraphs::RParagraphs,
    vector_reader_service: vectors::RVectors,
    relation_reader_service: relations::RRelations,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardReaderService {
    pub fn document_version(&self) -> DocumentService {
        match self.document_service_version {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            i => panic!("Unknown document version {i}"),
        }
    }
    pub fn paragraph_version(&self) -> ParagraphService {
        match self.paragraph_service_version {
            0 => ParagraphService::ParagraphV0,
            1 => ParagraphService::ParagraphV1,
            i => panic!("Unknown paragraph version {i}"),
        }
    }
    pub fn vector_version(&self) -> VectorService {
        match self.vector_service_version {
            0 => VectorService::VectorV0,
            1 => VectorService::VectorV1,
            i => panic!("Unknown vector version {i}"),
        }
    }
    pub fn relation_version(&self) -> RelationService {
        match self.relation_service_version {
            0 => RelationService::RelationV0,
            1 => RelationService::RelationV1,
            i => panic!("Unknown relation version {i}"),
        }
    }
    pub async fn get_info(&self) -> StatsData {
        self.reload_policy(true).await;
        let field_reader_service = self.field_reader_service.clone();
        let paragraph_reader_service = self.paragraph_reader_service.clone();
        let vector_reader_service = self.vector_reader_service.clone();
        let relation_reader_service = self.relation_reader_service.clone();
        let resources = tokio::task::spawn_blocking(move || field_reader_service.count());
        let paragraphs = tokio::task::spawn_blocking(move || paragraph_reader_service.count());
        let sentences = tokio::task::spawn_blocking(move || vector_reader_service.count());
        let relations = tokio::task::spawn_blocking(move || relation_reader_service.count());
        StatsData {
            resources: resources.await.unwrap(),
            paragraphs: paragraphs.await.unwrap(),
            sentences: sentences.await.unwrap(),
            relations: relations.await.unwrap(),
        }
    }

    pub async fn get_field_keys(&self) -> Vec<String> {
        self.reload_policy(true).await;
        let field_reader_service = self.field_reader_service.clone();
        tokio::task::spawn_blocking(move || field_reader_service.stored_ids())
            .await
            .unwrap()
    }
    pub async fn get_paragraphs_keys(&self) -> Vec<String> {
        self.reload_policy(true).await;
        let paragraph_reader_service = self.paragraph_reader_service.clone();
        tokio::task::spawn_blocking(move || paragraph_reader_service.stored_ids())
            .await
            .unwrap()
    }
    pub async fn get_vectors_keys(&self) -> Vec<String> {
        self.reload_policy(true).await;
        let vector_reader_service = self.vector_reader_service.clone();
        tokio::task::spawn_blocking(move || vector_reader_service.stored_ids())
            .await
            .unwrap()
    }
    pub async fn get_relations_keys(&self) -> Vec<String> {
        self.reload_policy(true).await;
        let relation_reader_service = self.relation_reader_service.clone();
        tokio::task::spawn_blocking(move || relation_reader_service.stored_ids())
            .await
            .unwrap()
    }
    pub async fn get_relations_edges(&self) -> EdgeList {
        self.reload_policy(true).await;
        let relation_reader_service = self.relation_reader_service.clone();
        tokio::task::spawn_blocking(move || relation_reader_service.get_edges())
            .await
            .unwrap()
    }
    pub async fn get_relations_types(&self) -> TypeList {
        self.reload_policy(true).await;
        let relation_reader_service = self.relation_reader_service.clone();
        tokio::task::spawn_blocking(move || relation_reader_service.get_node_types())
            .await
            .unwrap()
    }
    pub fn get_resources(&self) -> usize {
        self.field_reader_service.count()
    }

    /// Start the service
    pub async fn start(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldServiceConfiguration {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphServiceConfiguration {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorServiceConfiguration {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationServiceConfiguration {
            path: format!("{}/relations", shard_path),
        };
        let config = loop {
            match ShardConfig::open(&shard_path).await {
                None => task::yield_now().await,
                Some(config) => break config,
            }
        };
        let field_reader_service = fields::create_reader(&fsc, config.version_fields).await?;
        let paragraph_reader_service =
            paragraphs::create_reader(&psc, config.version_paragraphs).await?;
        let vector_reader_service = vectors::create_reader(&vsc, config.version_vectors).await?;
        let relation_reader_service =
            relations::create_reader(&rsc, config.version_relations).await?;
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader_service,
            paragraph_reader_service,
            vector_reader_service,
            relation_reader_service,
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    pub async fn open(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldServiceConfiguration {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphServiceConfiguration {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorServiceConfiguration {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationServiceConfiguration {
            path: format!("{}/relations", shard_path),
        };
        let config = ShardConfig::new(&shard_path).await;
        let field_reader_service = fields::open_reader(&fsc, config.version_fields).await?;
        let paragraph_reader_service =
            paragraphs::open_reader(&psc, config.version_paragraphs).await?;
        let vector_reader_service = vectors::open_reader(&vsc, config.version_vectors).await?;
        let relation_reader_service =
            relations::open_reader(&rsc, config.version_relations).await?;
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader_service,
            paragraph_reader_service,
            vector_reader_service,
            relation_reader_service,
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    pub async fn create(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldServiceConfiguration {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphServiceConfiguration {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorServiceConfiguration {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationServiceConfiguration {
            path: format!("{}/relations", shard_path),
        };
        let config = ShardConfig::new(&shard_path).await;
        let field_reader_service = fields::create_reader(&fsc, config.version_fields).await?;
        let paragraph_reader_service =
            paragraphs::create_reader(&psc, config.version_paragraphs).await?;
        let vector_reader_service = vectors::create_reader(&vsc, config.version_vectors).await?;
        let relation_reader_service =
            relations::create_reader(&rsc, config.version_relations).await?;
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader_service,
            paragraph_reader_service,
            vector_reader_service,
            relation_reader_service,
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    /// Stop the service
    pub async fn stop(&mut self) {
        info!("Stopping shard {}...", { &self.id });

        if let Err(e) = self.field_reader_service.stop().await {
            error!("Error stopping the field reader service: {}", e);
        }
        if let Err(e) = self.paragraph_reader_service.stop().await {
            error!("Error stopping the paragraph writer service: {}", e);
        }

        if let Err(e) = self.vector_reader_service.stop().await {
            error!("Error stopping the Vector service: {}", e);
        }
    }

    /// Return a list of queries to suggest from the original
    /// query. The query with more words will come first. `max_group`
    /// defines the limit of words a query can have.
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

    pub async fn suggest(&self, search_request: SuggestRequest) -> InternalResult<SuggestResponse> {
        // Search for entities related to the query.

        let prefixes =
            Self::split_suggest_query(search_request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);

        let relations_requests = prefixes
            .into_iter()
            .map(|prefix| RelationSearchRequest {
                id: String::default(),
                prefix,
                type_filters: vec![RelationFilter {
                    ntype: relation_node::NodeType::Entity as i32,
                    subtype: "".to_string(),
                }],
                depth: 10,
                ..Default::default()
            })
            .collect::<Vec<RelationSearchRequest>>();

        let mut relations_tasks = vec![];
        for relations_request in relations_requests {
            let relations_reader_service = self.relation_reader_service.clone();
            relations_tasks.push(task::spawn_blocking(move || {
                relations_reader_service.search(&relations_request)
            }))
        }
        info!("{}:{}", line!(), file!());

        let paragraph_request = ParagraphSearchRequest {
            body: search_request.body.clone(),
            filter: search_request.filter.clone(),
            page_number: 0,
            result_per_page: 10,
            timestamps: search_request.timestamps.clone(),
            reload: false,
            id: String::default(),
            uuid: String::default(),
            fields: Vec::default(),
            order: None,
            faceted: None,
        };

        let paragraph_reader_service = self.paragraph_reader_service.clone();
        let paragraph_task =
            task::spawn_blocking(move || paragraph_reader_service.search(&paragraph_request));
        info!("{}:{}", line!(), file!());

        let (rparagraph, rrelations) =
            try_join!(paragraph_task, try_join_all(relations_tasks)).unwrap();

        let rparagraph = rparagraph.unwrap();

        let entities = rrelations
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

    #[tracing::instrument(name = "ShardReaderService::search", skip(self, search_request))]
    pub async fn search(&self, search_request: SearchRequest) -> InternalResult<SearchResponse> {
        self.reload_policy(search_request.reload).await;
        let skip_paragraphs = !search_request.paragraph;
        let skip_fields = !search_request.document;
        let skip_vectors = search_request.body.is_empty()
            || search_request.result_per_page.eq(&0)
            || search_request.vector.is_empty();

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
        };

        let field_reader_service = self.field_reader_service.clone();
        let span = tracing::Span::current();
        let text_task = task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
                if !skip_fields {
                    Some(field_reader_service.search(&field_request))
                } else {
                    None
                }
            })
        });
        info!("{}:{}", line!(), file!());

        let paragraph_request = ParagraphSearchRequest {
            id: "".to_string(),
            uuid: "".to_string(),
            body: search_request.body.clone(),
            fields: search_request.fields.clone(),
            filter: search_request.filter.clone(),
            order: search_request.order.clone(),
            faceted: search_request.faceted.clone(),
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            timestamps: search_request.timestamps.clone(),
            reload: search_request.reload,
        };

        let paragraph_reader_service = self.paragraph_reader_service.clone();
        let span = tracing::Span::current();
        let paragraph_task = task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
                if !skip_paragraphs {
                    Some(paragraph_reader_service.search(&paragraph_request))
                } else {
                    None
                }
            })
        });
        info!("{}:{}", line!(), file!());

        let vector_request = VectorSearchRequest {
            id: "".to_string(),
            vector: search_request.vector.clone(),
            tags: search_request.fields.clone(),
            reload: search_request.reload,
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
        };
        let vector_reader_service = self.vector_reader_service.clone();
        let span = tracing::Span::current();
        let vector_task = task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "vector reader search"), || {
                if !skip_vectors {
                    Some(vector_reader_service.search(&vector_request))
                } else {
                    None
                }
            })
        });
        info!("{}:{}", line!(), file!());
        let (rtext, rparagraph, rvector) =
            try_join!(text_task, paragraph_task, vector_task,).unwrap();
        info!("{}:{}", line!(), file!());
        Ok(SearchResponse {
            document: rtext.transpose()?,
            paragraph: rparagraph.transpose()?,
            vector: rvector.transpose()?,
        })
    }

    #[tracing::instrument(
        name = "ShardReaderService::paragraph_search",
        skip(self, search_request)
    )]
    pub async fn paragraph_search(
        &self,
        search_request: ParagraphSearchRequest,
    ) -> InternalResult<ParagraphSearchResponse> {
        self.reload_policy(search_request.reload).await;
        let paragraph_reader_service = self.paragraph_reader_service.clone();
        let span = tracing::Span::current();
        task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
                paragraph_reader_service.search(&search_request)
            })
        })
        .await
        .unwrap()
    }

    #[tracing::instrument(
        name = "ShardReaderService::document_search",
        skip(self, search_request)
    )]
    pub async fn document_search(
        &self,
        search_request: DocumentSearchRequest,
    ) -> InternalResult<DocumentSearchResponse> {
        self.reload_policy(search_request.reload).await;
        let field_reader_service = self.field_reader_service.clone();
        let span = tracing::Span::current();
        task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
                field_reader_service.search(&search_request)
            })
        })
        .await
        .unwrap()
    }

    #[tracing::instrument(name = "ShardReaderService::vector_search", skip(self, search_request))]
    pub async fn vector_search(
        &self,
        search_request: VectorSearchRequest,
    ) -> InternalResult<VectorSearchResponse> {
        self.reload_policy(search_request.reload).await;
        let vector_reader_service = self.vector_reader_service.clone();
        let span = tracing::Span::current();
        task::spawn_blocking(move || {
            run_with_telemetry(info_span!(parent: &span, "vector reader search"), || {
                vector_reader_service.search(&search_request)
            })
        })
        .await
        .unwrap()
    }

    pub async fn relation_search(
        &self,
        search_request: RelationSearchRequest,
    ) -> InternalResult<RelationSearchResponse> {
        self.reload_policy(search_request.reload).await;
        let relation_reader_service = self.relation_reader_service.clone();
        task::spawn_blocking(move || relation_reader_service.search(&search_request))
            .await
            .unwrap()
    }

    async fn reload_policy(&self, trigger: bool) {
        let elapsed = self
            .creation_time
            .read()
            .unwrap()
            .elapsed()
            .unwrap()
            .as_millis();
        if trigger || elapsed >= RELOAD_PERIOD {
            *self.creation_time.write().unwrap() = SystemTime::now();
            let vector_reader_service = self.vector_reader_service.clone();
            task::spawn_blocking(move || vector_reader_service.reload())
                .await
                .unwrap()
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
            ShardReaderService::split_suggest_query(query.clone(), 2),
            vec!["multiple words", "words"]
        );
    }
}
