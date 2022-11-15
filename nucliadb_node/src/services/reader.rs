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
    // relation_node,
    DocumentSearchRequest,
    DocumentSearchResponse,
    EdgeList,
    ParagraphSearchRequest,
    ParagraphSearchResponse,
    RelatedEntities,
    RelationSearchRequest,
    RelationSearchResponse,
    SearchRequest,
    SearchResponse,
    SuggestRequest,
    SuggestResponse,
    TypeList,
    VectorSearchRequest,
    VectorSearchResponse,
};
use nucliadb_services::*;
// use rayon::prelude::*;
use tracing::*;

use crate::config::Configuration;
use crate::services::config::ShardConfig;
use crate::stats::StatsData;
use crate::telemetry::run_with_telemetry;

const RELOAD_PERIOD: u128 = 5000;
const FIXED_VECTORS_RESULTS: usize = 10;
// const MAX_SUGGEST_COMPOUND_WORDS: usize = 3;

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
    pub fn get_info(&self) -> StatsData {
        self.reload_policy(true);
        let resources = self.field_reader.count();
        let paragraphs = self.paragraph_reader.count();
        let sentences = self.vector_reader.count();
        let relations = self.relation_reader.count();
        StatsData {
            resources,
            paragraphs,
            sentences,
            relations,
        }
    }

    pub fn get_field_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.field_reader.stored_ids()
    }
    pub fn get_paragraphs_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.paragraph_reader.stored_ids()
    }
    pub fn get_vectors_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.vector_reader.stored_ids()
    }
    pub fn get_relations_keys(&self) -> Vec<String> {
        self.reload_policy(true);
        self.relation_reader.stored_ids()
    }
    pub fn get_relations_edges(&self) -> EdgeList {
        self.reload_policy(true);
        self.relation_reader.get_edges()
    }
    pub fn get_relations_types(&self) -> TypeList {
        self.reload_policy(true);
        self.relation_reader.get_node_types()
    }
    pub fn get_resources(&self) -> usize {
        self.field_reader.count()
    }

    /// Start the service
    pub fn start(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldConfig {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphConfig {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationConfig {
            path: format!("{}/relations", shard_path),
        };

        let config = ShardConfig::new(&shard_path);
        let mut fields = None;
        let mut paragraphs = None;
        let mut vectors = None;
        let mut relations = None;
        rayon::scope(|s| {
            s.spawn(|_| fields = Some(fields::create_reader(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::create_reader(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::create_reader(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::create_reader(&psc, config.version_paragraphs))
            });
        });
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader: fields.transpose()?.unwrap(),
            paragraph_reader: paragraphs.transpose()?.unwrap(),
            vector_reader: vectors.transpose()?.unwrap(),
            relation_reader: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    pub fn open(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldConfig {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphConfig {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationConfig {
            path: format!("{}/relations", shard_path),
        };
        let config = ShardConfig::new(&shard_path);
        let mut fields = None;
        let mut paragraphs = None;
        let mut vectors = None;
        let mut relations = None;
        rayon::scope(|s| {
            s.spawn(|_| fields = Some(fields::open_reader(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::open_reader(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::open_reader(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::open_reader(&psc, config.version_paragraphs))
            });
        });
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader: fields.transpose()?.unwrap(),
            paragraph_reader: paragraphs.transpose()?.unwrap(),
            vector_reader: vectors.transpose()?.unwrap(),
            relation_reader: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    pub fn create(id: &str) -> InternalResult<ShardReaderService> {
        let shard_path = Configuration::shards_path_id(id);
        match Path::new(&shard_path).exists() {
            true => info!("Loading shard with id {}", id),
            false => info!("Creating new shard with id {}", id),
        }

        let fsc = FieldConfig {
            path: format!("{}/text", shard_path),
        };

        let psc = ParagraphConfig {
            path: format!("{}/paragraph", shard_path),
        };

        let vsc = VectorConfig {
            no_results: Some(FIXED_VECTORS_RESULTS),
            path: format!("{}/vectors", shard_path),
        };
        let rsc = RelationConfig {
            path: format!("{}/relations", shard_path),
        };
        let config = ShardConfig::new(&shard_path);
        let mut fields = None;
        let mut paragraphs = None;
        let mut vectors = None;
        let mut relations = None;
        rayon::scope(|s| {
            s.spawn(|_| fields = Some(fields::create_reader(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::create_reader(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::create_reader(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::create_reader(&psc, config.version_paragraphs))
            });
        });
        Ok(ShardReaderService {
            id: id.to_string(),
            creation_time: RwLock::new(SystemTime::now()),
            field_reader: fields.transpose()?.unwrap(),
            paragraph_reader: paragraphs.transpose()?.unwrap(),
            vector_reader: vectors.transpose()?.unwrap(),
            relation_reader: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    /// Stop the service
    pub fn stop(&self) {
        info!("Stopping shard {}...", { &self.id });
        let mut field_r = Ok(());
        let mut paragraph_r = Ok(());
        let mut vector_r = Ok(());
        let mut relation_r = Ok(());
        rayon::scope(|s| {
            s.spawn(|_| field_r = self.field_reader.stop());
            s.spawn(|_| paragraph_r = self.paragraph_reader.stop());
            s.spawn(|_| vector_r = self.vector_reader.stop());
            s.spawn(|_| relation_r = self.relation_reader.stop());
        });
        if let Err(e) = field_r {
            error!("Error stopping the field reader service: {}", e);
        }
        if let Err(e) = paragraph_r {
            error!("Error stopping the paragraph reader service: {}", e);
        }

        if let Err(e) = vector_r {
            error!("Error stopping the Vector reader service: {}", e);
        }
        if let Err(e) = relation_r {
            error!("Error stopping the Relation reader service: {}", e);
        }
        info!("Shard stopped {}...", { &self.id });
    }

    /// Return a list of queries to suggest from the original
    /// query. The query with more words will come first. `max_group`
    /// defines the limit of words a query can have.
    #[allow(unused)]
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

    pub fn suggest(&self, request: SuggestRequest) -> InternalResult<SuggestResponse> {
        // Search for entities related to the query.

        // let prefixes =
        //     Self::split_suggest_query(search_request.body.clone(), MAX_SUGGEST_COMPOUND_WORDS);

        // let relations = prefixes.par_iter().map(|prefix| {
        //     let filter = RelationFilter {
        //         ntype: relation_node::NodeType::Entity as i32,
        //         subtype: "".to_string(),
        //     };
        //     let request = RelationSearchRequest {
        //         id: String::default(),
        //         prefix: prefix.clone(),
        //         type_filters: vec![filter],
        //         depth: 10,
        //         ..Default::default()
        //     };
        //     self.relation_reader.search(&request)
        // });
        // info!("{}:{}", line!(), file!());

        let paragraph_reader_service = self.paragraph_reader.clone();
        let paragraph_task = move || paragraph_reader_service.suggest(&request);
        info!("{}:{}", line!(), file!());
        let rparagraph = paragraph_task()?;
        // let (rparagraph, rrelations) =
        //     rayon::join(paragraph_task, || relations.collect::<Vec<_>>());

        // let rparagraph = rparagraph.unwrap();

        // let entities = rrelations
        //     .into_iter()
        //     .flat_map(|relation| {
        //         relation
        //             .unwrap()
        //             .neighbours
        //             .iter()
        //             .map(|relation_node| relation_node.value.clone())
        //             .collect::<Vec<String>>()
        //     })
        //     .collect::<Vec<String>>();

        Ok(SuggestResponse {
            query: rparagraph.query,
            total: rparagraph.total,
            results: rparagraph.results,
            ematches: rparagraph.ematches,
            entities: Some(RelatedEntities {
                total: 0,
                entities: vec![],
            }),
        })
    }

    #[tracing::instrument(name = "ShardReaderService::search", skip(self, search_request))]
    pub fn search(&self, search_request: SearchRequest) -> InternalResult<SearchResponse> {
        self.reload_policy(search_request.reload);
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

        let field_reader_service = self.field_reader.clone();
        let span = tracing::Span::current();
        let text_task = move || {
            run_with_telemetry(info_span!(parent: &span, "field reader search"), || {
                if !skip_fields {
                    Some(field_reader_service.search(&field_request))
                } else {
                    None
                }
            })
        };
        info!("{}:{}", line!(), file!());

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
        };

        let paragraph_reader_service = self.paragraph_reader.clone();
        let span = tracing::Span::current();
        let paragraph_task = move || {
            run_with_telemetry(info_span!(parent: &span, "paragraph reader search"), || {
                if !skip_paragraphs {
                    Some(paragraph_reader_service.search(&paragraph_request))
                } else {
                    None
                }
            })
        };
        info!("{}:{}", line!(), file!());

        let vector_request = VectorSearchRequest {
            id: "".to_string(),
            vector: search_request.vector.clone(),
            tags: search_request.fields.clone(),
            reload: search_request.reload,
            page_number: search_request.page_number,
            result_per_page: search_request.result_per_page,
            with_duplicates: true,
        };
        let vector_reader_service = self.vector_reader.clone();
        let span = tracing::Span::current();
        let vector_task = move || {
            run_with_telemetry(info_span!(parent: &span, "vector reader search"), || {
                if !skip_vectors {
                    Some(vector_reader_service.search(&vector_request))
                } else {
                    None
                }
            })
        };
        info!("{}:{}", line!(), file!());
        let mut rtext = None;
        let mut rparagraph = None;
        let mut rvector = None;
        rayon::scope(|s| {
            s.spawn(|_| rtext = text_task());
            s.spawn(|_| rparagraph = paragraph_task());
            s.spawn(|_| rvector = vector_task());
        });
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

    #[tracing::instrument(
        name = "ShardReaderService::document_search",
        skip(self, search_request)
    )]
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

    #[tracing::instrument(name = "ShardReaderService::vector_search", skip(self, search_request))]
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

    pub fn relation_search(
        &self,
        search_request: RelationSearchRequest,
    ) -> InternalResult<RelationSearchResponse> {
        self.reload_policy(search_request.reload);
        self.relation_reader.search(&search_request)
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
            *self.creation_time.write().unwrap() = SystemTime::now();
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
