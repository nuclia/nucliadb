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
use nucliadb_core::protos::{
    DeleteGraphNodes, JoinGraph, NewShardRequest, OpStatus, Resource, ResourceId, VectorSetId,
    VectorSimilarity,
};
use nucliadb_core::thread;
use nucliadb_core::tracing::{self, *};

use super::shard_disk_structure::*;
use crate::services::versions::Versions;
use crate::shard_metadata::ShardMetadata;
use crate::telemetry::run_with_telemetry;

#[derive(Debug)]
pub struct ShardWriterService {
    pub metadata: ShardMetadata,
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
    fn initialize(
        id: String,
        path: &Path,
        metadata: ShardMetadata,
        tsc: TextConfig,
        psc: ParagraphConfig,
        vsc: VectorConfig,
        rsc: RelationConfig,
    ) -> NodeResult<ShardWriterService> {
        let versions = Versions::load_or_create(&path.join(VERSION_FILE))?;
        let text_task = || Some(versions.get_texts_writer(&tsc));
        let paragraph_task = || Some(versions.get_paragraphs_writer(&psc));
        let vector_task = || Some(versions.get_vectors_writer(&vsc));
        let relation_task = || Some(versions.get_relations_writer(&rsc));

        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text start");
        let text_task = || run_with_telemetry(info, text_task);
        let info = info_span!(parent: &span, "paragraph start");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);
        let info = info_span!(parent: &span, "vector start");
        let vector_task = || run_with_telemetry(info, vector_task);
        let info = info_span!(parent: &span, "relation start");
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
            metadata,
            path: path.to_path_buf(),
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
    pub fn clean_and_create(id: String, path: &Path) -> NodeResult<ShardWriterService> {
        let metadata = ShardMetadata::open(&path.join(METADATA_FILE))?;
        std::fs::remove_dir_all(path)?;
        std::fs::create_dir_all(path)?;
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let vsc = VectorConfig {
            no_results: None,
            similarity: metadata.similarity(),
            path: path.join(VECTORS_DIR),
            vectorset: path.join(VECTORSET_DIR),
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
        };
        ShardWriterService::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }
    pub fn new(
        id: String,
        path: &Path,
        request: &NewShardRequest,
    ) -> NodeResult<ShardWriterService> {
        std::fs::create_dir_all(path)?;
        let metadata_path = path.join(METADATA_FILE);
        let similarity = request.similarity();
        let metadata = ShardMetadata::from(request.clone());
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let vsc = VectorConfig {
            no_results: None,
            similarity: Some(similarity),
            path: path.join(VECTORS_DIR),
            vectorset: path.join(VECTORSET_DIR),
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
        };

        metadata.serialize(&metadata_path)?;
        ShardWriterService::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }

    pub fn open(id: String, path: &Path) -> NodeResult<ShardWriterService> {
        let metadata_path = path.join(METADATA_FILE);
        let metadata = ShardMetadata::open(&metadata_path)?;
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let vsc = VectorConfig {
            no_results: None,
            similarity: None,
            path: path.join(VECTORS_DIR),
            vectorset: path.join(VECTORSET_DIR),
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
        };
        ShardWriterService::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }

    #[tracing::instrument(skip_all)]
    pub fn stop(&mut self) {
        info!("Stopping shard {}...", { &self.id });
        let texts = self.text_writer.clone();
        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let relations = self.relation_writer.clone();

        let text_task = move || text_write(&texts).stop();
        let paragraph_task = move || paragraph_write(&paragraphs).stop();
        let vector_task = move || vector_write(&vectors).stop();
        let relation_task = move || relation_write(&relations).stop();

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
        let text_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            info!("Field service starts set_resource");
            let mut writer = text_write(&text_writer_service);
            let result = writer.set_resource(&field_resource);
            info!("Field service ends set_resource");
            result
        };

        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            info!("Paragraph service starts set_resource");
            let mut writer = paragraph_write(&paragraph_writer_service);
            let result = writer.set_resource(&paragraph_resource);
            info!("Paragraph service ends set_resource");
            result
        };

        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            info!("Vector service starts set_resource");
            let mut writer = vector_write(&vector_writer_service);
            let result = writer.set_resource(&vector_resource);
            info!("Vector service ends set_resource");
            result
        };

        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            info!("Relation service starts set_resource");
            let mut writer = relation_write(&relation_writer_service);
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
            let mut writer = text_write(&text_writer_service);
            writer.delete_resource(&field_resource)
        };
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            let mut writer = paragraph_write(&paragraph_writer_service);
            writer.delete_resource(&paragraph_resource)
        };
        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            let mut writer = vector_write(&vector_writer_service);
            writer.delete_resource(&vector_resource)
        };
        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            let mut writer = relation_write(&relation_writer_service);
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
    pub fn get_opstatus(&self) -> NodeResult<OpStatus> {
        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let texts = self.text_writer.clone();
        let span = tracing::Span::current();
        let info = info_span!(parent: &span, "text count");
        let text_task = || run_with_telemetry(info, move || text_read(&texts).count());
        let info = info_span!(parent: &span, "paragraph count");
        let paragraph_task =
            || run_with_telemetry(info, move || paragraph_read(&paragraphs).count());
        let info = info_span!(parent: &span, "vector count");
        let vector_task = || run_with_telemetry(info, move || vector_read(&vectors).count());

        let mut text_result = Ok(0);
        let mut paragraph_result = Ok(0);
        let mut vector_result = Ok(0);
        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            s.spawn(|_| vector_result = vector_task());
        });
        Ok(OpStatus {
            shard_id: self.id.clone(),
            count: text_result? as u64,
            count_paragraphs: paragraph_result? as u64,
            count_sentences: vector_result? as u64,
            ..Default::default()
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let reader = vector_read(&self.vector_writer);
        let keys = reader.list_vectorsets()?;
        Ok(keys)
    }
    #[tracing::instrument(skip_all)]
    pub fn add_vectorset(
        &self,
        setid: &VectorSetId,
        similarity: VectorSimilarity,
    ) -> NodeResult<()> {
        let mut writer = vector_write(&self.vector_writer);
        writer.add_vectorset(setid, similarity)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(&self, setid: &VectorSetId) -> NodeResult<()> {
        let mut writer = vector_write(&self.vector_writer);
        writer.remove_vectorset(setid)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn delete_relation_nodes(&self, nodes: &DeleteGraphNodes) -> NodeResult<()> {
        let mut writer = relation_write(&self.relation_writer);
        writer.delete_nodes(nodes)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn join_relations_graph(&self, graph: &JoinGraph) -> NodeResult<()> {
        let mut writer = relation_write(&self.relation_writer);
        writer.join_graph(graph)?;
        Ok(())
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_count(&self) -> NodeResult<usize> {
        paragraph_read(&self.paragraph_writer).count()
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_count(&self) -> NodeResult<usize> {
        vector_read(&self.vector_writer).count()
    }
    #[tracing::instrument(skip_all)]
    pub fn text_count(&self) -> NodeResult<usize> {
        text_read(&self.text_writer).count()
    }
    #[tracing::instrument(skip_all)]
    pub fn gc(&self) -> NodeResult<()> {
        vector_write(&self.vector_writer).garbage_collection()
    }
}
