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
use std::sync::{Arc, RwLock};

use futures::try_join;
use nucliadb_protos::{Resource, ResourceId, SetVectorFieldRequest};
use tracing::*;

use crate::config::Configuration;
use crate::result::InternalResult;
use crate::services::field::config::FieldServiceConfiguration;
use crate::services::field::writer::FieldWriterService;
use crate::services::paragraph::config::ParagraphServiceConfiguration;
use crate::services::paragraph::writer::ParagraphWriterService;
use crate::services::service::*;
use crate::services::vector::config::VectorServiceConfiguration;
use crate::services::vector::writer::VectorWriterService;

#[derive(Debug)]
pub struct ShardWriterService {
    pub id: String,

    field_writer_service: Arc<RwLock<FieldWriterService>>,
    paragraph_writer_service: Arc<RwLock<ParagraphWriterService>>,
    vector_writer_service: Arc<RwLock<VectorWriterService>>,
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

        let field_writer_service = FieldWriterService::start(&fsc).await?;
        let paragraph_writer_service = ParagraphWriterService::start(&psc).await?;
        let vector_writer_service = VectorWriterService::start(&vsc).await?;

        Ok(ShardWriterService {
            id: id.to_string(),
            field_writer_service: Arc::new(RwLock::new(field_writer_service)),
            paragraph_writer_service: Arc::new(RwLock::new(paragraph_writer_service)),
            vector_writer_service: Arc::new(RwLock::new(vector_writer_service)),
            document_service_version: 0,
            paragraph_service_version: 0,
            vector_service_version: 0,
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

    pub async fn set_resource(&mut self, resource: &Resource) -> InternalResult<()> {
        let field_writer_service = self.field_writer_service.clone();
        let field_resource = resource.clone();
        let text_task = tokio::task::spawn_blocking(move || {
            let mut writer = field_writer_service.write().unwrap();
            writer.set_resource(&field_resource)
        });
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer_service.clone();
        let paragraph_task = tokio::task::spawn_blocking(move || {
            let mut writer = paragraph_writer_service.write().unwrap();
            writer.set_resource(&paragraph_resource)
        });
        let vector_writer_service = self.vector_writer_service.clone();
        let vector_resource = resource.clone();
        let vector_task = tokio::task::spawn_blocking(move || {
            let mut writer = vector_writer_service.write().unwrap();
            writer.set_resource(&vector_resource)
        });
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
    pub async fn set_vector_field(
        &self,
        _resource: &SetVectorFieldRequest,
    ) -> Result<(), Box<dyn std::error::Error>> {
        todo!();
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
