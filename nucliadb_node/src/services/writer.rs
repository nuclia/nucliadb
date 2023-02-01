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

use std::path::{Path, PathBuf};

use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_core::protos::{DeleteGraphNodes, JoinGraph, Resource, ResourceId, VectorSetId};
use nucliadb_core::thread;
use nucliadb_core::tracing::{self, *};

use super::shard_disk_structure::*;
use crate::services::versions::Versions;
use crate::telemetry::run_with_telemetry;

#[derive(Debug)]
pub struct ShardWriterService {
    pub id: String,
    pub path: PathBuf,
    text_writer: TextsWriterPointer,
    paragraph_writer: ParagraphsWriterPointer,
    vector_writer: VectorsWriterPointer,
    relation_writer: RelationsWriterPointer,
    document_service_version: i32,
    paragraph_service_version: i32,
    vector_service_version: i32,
    relation_service_version: i32,
}

impl ShardWriterService {
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
    pub fn new(id: String, shard_path: &Path) -> NodeResult<ShardWriterService> {
        let tsc = TextConfig {
            path: shard_path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: shard_path.join(PARAGRAPHS_DIR),
        };

        let vsc = VectorConfig {
            no_results: None,
            path: shard_path.join(VECTORS_DIR),
            vectorset: shard_path.join(VECTORSET_DIR),
        };
        let rsc = RelationConfig {
            path: shard_path.join(RELATIONS_DIR),
        };
        let versions = Versions::load_or_create(&shard_path.join(VERSION_FILE))?;
        let text_task = || Some(versions.get_texts_writer(&tsc));
        let paragraph_task = || Some(versions.get_paragraphs_writer(&psc));
        let vector_task = || Some(versions.get_vectors_writer(&vsc));
        let relation_task = || Some(versions.get_relations_writer(&rsc));

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

        Ok(ShardWriterService {
            id,
            path: shard_path.to_path_buf(),
            text_writer: fields.unwrap(),
            paragraph_writer: paragraphs.unwrap(),
            vector_writer: vectors.unwrap(),
            relation_writer: relations.unwrap(),
            document_service_version: versions.version_texts() as i32,
            paragraph_service_version: versions.version_paragraphs() as i32,
            vector_service_version: versions.version_vectors() as i32,
            relation_service_version: versions.version_relations() as i32,
        })
    }
    #[tracing::instrument(skip_all)]
    pub fn stop(&mut self) {
        info!("Stopping shard {}...", { &self.id });
        let texts = self.text_writer.clone();
        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let relations = self.relation_writer.clone();

        let text_task = move || texts.write().unwrap().stop();
        let paragraph_task = move || paragraphs.write().unwrap().stop();
        let vector_task = move || vectors.write().unwrap().stop();
        let relation_task = move || relations.write().unwrap().stop();

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
            error!("Error stopping the Field writer service: {}", e);
        }
        if let Err(e) = paragraph_result {
            error!("Error stopping the Paragraph writer service: {}", e);
        }
        if let Err(e) = vector_result {
            error!("Error stopping the Vector writer service: {}", e);
        }
        if let Err(e) = relation_result {
            error!("Error stopping the Relation writer service: {}", e);
        }
        info!("Shard stopped {}...", { &self.id });
    }

    #[tracing::instrument(skip_all)]
    pub fn set_resource(&mut self, resource: &Resource) -> NodeResult<()> {
        let field_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            info!("Field service starts set_resource");
            let mut writer = field_writer_service.write().unwrap();
            let result = writer.set_resource(&field_resource);
            info!("Field service ends set_resource");
            result
        };

        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            info!("Paragraph service starts set_resource");
            let mut writer = paragraph_writer_service.write().unwrap();
            let result = writer.set_resource(&paragraph_resource);
            info!("Paragraph service ends set_resource");
            result
        };

        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            info!("Vector service starts set_resource");
            let mut writer = vector_writer_service.write().unwrap();
            let result = writer.set_resource(&vector_resource);
            info!("Vector service ends set_resource");
            result
        };

        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            info!("Relation service starts set_resource");
            let mut writer = relation_writer_service.write().unwrap();
            let result = writer.set_resource(&relation_resource);
            info!("Relation service ends set_resource");
            result
        };

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text set_resource");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph set_resource");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector set_resource");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation set_resource");
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

        text_result?;
        paragraph_result?;
        vector_result?;
        relation_result?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn remove_resource(&mut self, resource: &ResourceId) -> NodeResult<()> {
        let text_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            let mut writer = text_writer_service.write().unwrap();
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

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text remove");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph remove");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector remove");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation remove");
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
        text_result?;
        paragraph_result?;
        vector_result?;
        relation_result?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let reader = self.vector_writer.read().unwrap();
        let keys = reader.list_vectorsets()?;
        Ok(keys)
    }
    #[tracing::instrument(skip_all)]
    pub fn add_vectorset(&self, setid: &VectorSetId) -> NodeResult<()> {
        let mut writer = self.vector_writer.write().unwrap();
        writer.add_vectorset(setid)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(&self, setid: &VectorSetId) -> NodeResult<()> {
        let mut writer = self.vector_writer.write().unwrap();
        writer.remove_vectorset(setid)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn delete_relation_nodes(&self, nodes: &DeleteGraphNodes) -> NodeResult<()> {
        let mut writer = self.relation_writer.write().unwrap();
        writer.delete_nodes(nodes)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn join_relations_graph(&self, graph: &JoinGraph) -> NodeResult<()> {
        let mut writer = self.relation_writer.write().unwrap();
        writer.join_graph(graph)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn count(&self) -> usize {
        self.text_writer.read().unwrap().count()
    }
    #[tracing::instrument(skip_all)]
    pub fn gc(&self) -> NodeResult<()> {
        let vector_writer_service = self.vector_writer.clone();
        let mut writer = vector_writer_service.write().unwrap();
        writer.garbage_collection();
        Ok(())
    }
}
