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

use futures::try_join;
use nucliadb_protos::{Resource, ResourceId};
use nucliadb_services::*;
use tracing::*;

use crate::config::Configuration;
use crate::services::config::ShardConfig;
use crate::telemetry::run_with_telemetry;

#[derive(Debug)]
pub struct ShardWriterService {
    pub id: String,

    field_writer_service: fields::WFields,
    paragraph_writer_service: paragraphs::WParagraphs,
    vector_writer_service: vectors::WVectors,
    pub document_service_version: i32,
    pub paragraph_service_version: i32,
    pub vector_service_version: i32,
    pub relation_service_version: i32,
}

impl ShardWriterService {
    /// Start the service
    pub async fn start(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
            path: format!("{}/vectors", shard_path),
        };
        let config = ShardConfig::new(&shard_path).await;
        let field_writer_service = fields::create_writer(&fsc, config.version_fields).await?;
        let paragraph_writer_service =
            paragraphs::create_writer(&psc, config.version_paragraphs).await?;
        let vector_writer_service = vectors::create_writer(&vsc, config.version_vectors).await?;

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer_service,
            paragraph_writer_service,
            vector_writer_service,
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_paragraphs as i32,
            relation_service_version: 0,
        })
    }
    pub async fn new(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
            path: format!("{}/vectors", shard_path),
        };
        let config = ShardConfig::new(&shard_path).await;
        let field_writer_service = fields::create_writer(&fsc, config.version_fields).await?;
        let paragraph_writer_service =
            paragraphs::create_writer(&psc, config.version_paragraphs).await?;

        let vector_writer_service = vectors::create_writer(&vsc, config.version_vectors).await?;

        Ok(ShardWriterService {
            field_writer_service,
            paragraph_writer_service,
            vector_writer_service,
            id: id.to_string(),
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_paragraphs as i32,
            relation_service_version: 0,
        })
    }
    pub async fn open(id: &str) -> InternalResult<ShardWriterService> {
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
            no_results: None,
            path: format!("{}/vectors", shard_path),
        };
        let config = ShardConfig::new(&shard_path).await;
        let field_writer_service = fields::open_writer(&fsc, config.version_fields).await?;
        let paragraph_writer_service =
            paragraphs::open_writer(&psc, config.version_paragraphs).await?;
        let vector_writer_service = vectors::open_writer(&vsc, config.version_vectors).await?;

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer_service,
            paragraph_writer_service,
            vector_writer_service,
            document_service_version: config.version_fields as i32,
            paragraph_service_version: config.version_paragraphs as i32,
            vector_service_version: config.version_paragraphs as i32,
            relation_service_version: 0,
        })
    }

    pub async fn stop(&mut self) {
        info!("Stopping shard {}...", { &self.id });
        if let Err(e) = self.paragraph_writer_service.write().unwrap().stop().await {
            error!("Error stopping the paragraph writer service: {}", e);
        }
        if let Err(e) = self.field_writer_service.write().unwrap().stop().await {
            error!("Error stopping the field writer service: {}", e);
        }

        if let Err(e) = self.vector_writer_service.write().unwrap().stop().await {
            error!("Error stopping the Vector service: {}", e);
        }
    }

    #[tracing::instrument(name = "ShardWriterService::set_resource", skip(self, resource))]
    pub async fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        let field_writer_service = self.field_writer_service.clone();
        let field_resource = resource.clone();
        info!("Field service starts");
        let span = tracing::Span::current();
        let text_task = tokio::task::spawn_blocking(move || {
            run_with_telemetry(
                info_span!(parent: &span, "field writer set resource"),
                || {
                    let mut writer = field_writer_service.write().unwrap();
                    writer.set_resource(&field_resource)
                },
            )
        });
        info!("Field service ends");
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer_service.clone();
        info!("Paragraph service starts");
        let span = tracing::Span::current();
        let paragraph_task = tokio::task::spawn_blocking(move || {
            run_with_telemetry(
                info_span!(parent: &span, "paragraph writer set resource"),
                || {
                    let mut writer = paragraph_writer_service.write().unwrap();
                    writer.set_resource(&paragraph_resource)
                },
            )
        });
        info!("Paragraph service ends");
        let vector_writer_service = self.vector_writer_service.clone();
        let vector_resource = resource.clone();
        info!("Vector service starts");
        let span = tracing::Span::current();
        let vector_task = tokio::task::spawn_blocking(move || {
            run_with_telemetry(
                info_span!(parent: &span, "vector writer set resource"),
                || {
                    let mut writer = vector_writer_service.write().unwrap();
                    writer.set_resource(&vector_resource)
                },
            )
        });
        info!("Vector service ends");
        let (rtext, rparagraph, rvector) =
            try_join!(text_task, paragraph_task, vector_task).unwrap();
        rtext?;
        rparagraph?;
        rvector?;
        Ok(())
    }

    pub async fn remove_resource(&mut self, resource: &ResourceId) -> InternalResult<()> {
        let field_writer_service = self.field_writer_service.clone();
        let field_resource = resource.clone();
        let text_task = tokio::task::spawn_blocking(move || {
            let mut writer = field_writer_service.write().unwrap();
            writer.delete_resource(&field_resource)
        });
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer_service.clone();
        let paragraph_task = tokio::task::spawn_blocking(move || {
            let mut writer = paragraph_writer_service.write().unwrap();
            writer.delete_resource(&paragraph_resource)
        });
        let vector_writer_service = self.vector_writer_service.clone();
        let vector_resource = resource.clone();
        let vector_task = tokio::task::spawn_blocking(move || {
            let mut writer = vector_writer_service.write().unwrap();
            writer.delete_resource(&vector_resource)
        });
        let (rtext, rparagraph, rvector) =
            try_join!(text_task, paragraph_task, vector_task).unwrap();
        rtext?;
        rparagraph?;
        rvector?;
        Ok(())
    }
    pub async fn delete(&self) -> Result<(), std::io::Error> {
        let shard_path = Configuration::shards_path_id(&self.id);
        info!("Deleting {}", shard_path);
        std::fs::remove_dir_all(shard_path)
    }

    pub fn count(&self) -> usize {
        self.field_writer_service.read().unwrap().count()
    }

    pub async fn gc(&self) -> InternalResult<()> {
        let vector_writer_service = self.vector_writer_service.clone();
        let vector_task = tokio::task::spawn_blocking(move || {
            let mut writer = vector_writer_service.write().unwrap();
            writer.garbage_collection();
        });
        try_join!(vector_task).unwrap();
        Ok(())
    }
}
