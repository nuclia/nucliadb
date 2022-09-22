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

use nucliadb_protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_services::*;
use tracing::*;

use crate::config::Configuration;
use crate::services::config::ShardConfig;
use crate::telemetry::run_with_telemetry;

#[derive(Debug)]
pub struct ShardWriterService {
    pub id: String,
    field_writer: fields::WFields,
    paragraph_writer: paragraphs::WParagraphs,
    vector_writer: vectors::WVectors,
    relation_writer: relations::WRelations,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardWriterService {
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
    /// Start the service
    pub fn start(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
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
            s.spawn(|_| fields = Some(fields::create_writer(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::create_writer(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::create_writer(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::create_writer(&psc, config.version_paragraphs))
            });
        });

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer: fields.transpose()?.unwrap(),
            paragraph_writer: paragraphs.transpose()?.unwrap(),
            vector_writer: vectors.transpose()?.unwrap(),
            relation_writer: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }
    pub fn new(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
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
            s.spawn(|_| fields = Some(fields::create_writer(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::create_writer(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::create_writer(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::create_writer(&psc, config.version_paragraphs))
            });
        });

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer: fields.transpose()?.unwrap(),
            paragraph_writer: paragraphs.transpose()?.unwrap(),
            vector_writer: vectors.transpose()?.unwrap(),
            relation_writer: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }
    pub fn open(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
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
            s.spawn(|_| fields = Some(fields::open_writer(&fsc, config.version_fields)));
            s.spawn(|_| vectors = Some(vectors::open_writer(&vsc, config.version_vectors)));
            s.spawn(|_| relations = Some(relations::open_writer(&rsc, config.version_relations)));
            s.spawn(|_| {
                paragraphs = Some(paragraphs::open_writer(&psc, config.version_paragraphs))
            });
        });

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer: fields.transpose()?.unwrap(),
            paragraph_writer: paragraphs.transpose()?.unwrap(),
            vector_writer: vectors.transpose()?.unwrap(),
            relation_writer: relations.transpose()?.unwrap(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_vectors as i32,
            relation_service_version: config.version_relations as i32,
        })
    }

    pub fn stop(&mut self) {
        info!("Stopping shard {}...", { &self.id });
        let fields = self.field_writer.clone();
        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let relations = self.relation_writer.clone();
        let mut field_r = Ok(());
        let mut paragraph_r = Ok(());
        let mut vector_r = Ok(());
        let mut relation_r = Ok(());
        rayon::scope(|s| {
            s.spawn(|_| field_r = fields.write().unwrap().stop());
            s.spawn(|_| paragraph_r = paragraphs.write().unwrap().stop());
            s.spawn(|_| vector_r = vectors.write().unwrap().stop());
            s.spawn(|_| relation_r = relations.write().unwrap().stop());
        });
        if let Err(e) = field_r {
            error!("Error stopping the field writer service: {}", e);
        }
        if let Err(e) = paragraph_r {
            error!("Error stopping the paragraph writer service: {}", e);
        }

        if let Err(e) = vector_r {
            error!("Error stopping the Vector writer service: {}", e);
        }
        if let Err(e) = relation_r {
            error!("Error stopping the Relation writer service: {}", e);
        }
        info!("Shard stopped {}...", { &self.id });
    }

    #[tracing::instrument(name = "ShardWriterService::set_resource", skip(self, resource))]
    pub fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        let field_writer_service = self.field_writer.clone();
        let field_resource = resource.clone();
        let span = tracing::Span::current();
        let text_task = move || {
            info!("Field service starts set_resource");
            let result = run_with_telemetry(
                info_span!(parent: &span, "field writer set resource"),
                || {
                    let mut writer = field_writer_service.write().unwrap();
                    writer.set_resource(&field_resource)
                },
            );
            info!("Field service ends set_resource");
            result
        };

        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let span = tracing::Span::current();
        let paragraph_task = move || {
            info!("Paragraph service starts set_resource");
            let result = run_with_telemetry(
                info_span!(parent: &span, "paragraph writer set resource"),
                || {
                    let mut writer = paragraph_writer_service.write().unwrap();
                    writer.set_resource(&paragraph_resource)
                },
            );
            info!("Paragraph service ends set_resource");
            result
        };

        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let span = tracing::Span::current();
        let vector_task = move || {
            info!("Vector service starts set_resource");
            let result = run_with_telemetry(
                info_span!(parent: &span, "vector writer set resource"),
                || {
                    let mut writer = vector_writer_service.write().unwrap();
                    writer.set_resource(&vector_resource)
                },
            );
            info!("Vector service ends set_resource");
            result
        };

        // let relation_writer_service = self.relation_writer.clone();
        // let relation_resource = resource.clone();
        // let relation_task = move || {
        //     info!("Relation service starts set_resource");
        //     let mut writer = relation_writer_service.write().unwrap();
        //     let result = writer.set_resource(&relation_resource);
        //     info!("Relation service ends set_resource");
        //     result
        //     Ok(())
        // };

        let mut text_result = Ok(());
        let mut paragraph_result = Ok(());
        let mut vector_result = Ok(());
        // let mut relation_result = Ok(());
        rayon::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
            // s.spawn(|_| relation_result = relation_task());
        });

        text_result?;
        paragraph_result?;
        vector_result?;
        Ok(())
    }

    pub fn remove_resource(&mut self, resource: &ResourceId) -> InternalResult<()> {
        let field_writer_service = self.field_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            let mut writer = field_writer_service.write().unwrap();
            writer.delete_resource(&field_resource)
        };
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            let mut writer = paragraph_writer_service.write().unwrap();
            writer.delete_resource(&paragraph_resource)
        };
        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            let mut writer = vector_writer_service.write().unwrap();
            writer.delete_resource(&vector_resource)
        };
        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            let mut writer = relation_writer_service.write().unwrap();
            writer.delete_resource(&relation_resource)
        };

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
        text_result?;
        paragraph_result?;
        vector_result?;
        relation_result?;
        Ok(())
    }
    pub fn delete(&self) -> Result<(), std::io::Error> {
        let shard_path = Configuration::shards_path_id(&self.id);
        info!("Deleting {}", shard_path);
        std::fs::remove_dir_all(shard_path)
    }

    pub fn count(&self) -> usize {
        self.field_writer.read().unwrap().count()
    }

    pub fn gc(&self) -> InternalResult<()> {
        let vector_writer_service = self.vector_writer.clone();
        let mut writer = vector_writer_service.write().unwrap();
        writer.garbage_collection();
        Ok(())
    }
}
