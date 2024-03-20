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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use nucliadb_core::paragraphs::*;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{DocumentService, ParagraphService, RelationService, VectorService};
use nucliadb_core::protos::{OpStatus, Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::relations::*;
use nucliadb_core::texts::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::*;
use nucliadb_core::{thread, IndexFiles};
use nucliadb_procs::measure;
use nucliadb_vectors::VectorErr;

use crate::disk_structure::*;
use crate::shards::metadata::ShardMetadata;
use crate::shards::versions::Versions;
use crate::telemetry::run_with_telemetry;

pub struct BlockingToken<'a>(MutexGuard<'a, ()>);

#[derive(Debug)]
pub struct ShardWriter {
    pub metadata: Arc<ShardMetadata>,
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
    pub gc_lock: tokio::sync::Mutex<()>, // lock to be able to do GC or not
    write_lock: Mutex<()>,               // be able to lock writes on the shard
}

impl ShardWriter {
    #[tracing::instrument(skip_all)]
    fn initialize(
        metadata: Arc<ShardMetadata>,
        tsc: TextConfig,
        psc: ParagraphConfig,
        vsc: VectorConfig,
        rsc: RelationConfig,
    ) -> NodeResult<ShardWriter> {
        let versions = Versions::load_or_create(&metadata.shard_path().join(VERSION_FILE), metadata.channel())?;
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

        Ok(ShardWriter {
            id: metadata.id(),
            path: metadata.shard_path(),
            metadata,
            text_writer: fields.unwrap(),
            paragraph_writer: paragraphs.unwrap(),
            vector_writer: vectors.unwrap(),
            relation_writer: relations.unwrap(),
            document_service_version: versions.version_texts() as i32,
            paragraph_service_version: versions.version_paragraphs() as i32,
            vector_service_version: versions.version_vectors() as i32,
            relation_service_version: versions.version_relations() as i32,
            gc_lock: tokio::sync::Mutex::new(()),
            write_lock: Mutex::new(()),
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn document_version(&self) -> DocumentService {
        match self.document_service_version {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            2 => DocumentService::DocumentV2,
            i => panic!("Unknown document version {i}"),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_version(&self) -> ParagraphService {
        match self.paragraph_service_version {
            0 => ParagraphService::ParagraphV0,
            1 => ParagraphService::ParagraphV1,
            2 => ParagraphService::ParagraphV2,
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
            2 => RelationService::RelationV2,
            i => panic!("Unknown relation version {i}"),
        }
    }

    #[measure(actor = "shard", metric = "new")]
    pub fn new(metadata: Arc<ShardMetadata>) -> NodeResult<ShardWriter> {
        let path = metadata.shard_path();
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel();

        let vsc = VectorConfig {
            similarity: Some(metadata.similarity()),
            path: path.join(VECTORS_DIR),
            vectorset: path.join(VECTORSET_DIR),
            channel,
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
            channel,
        };

        std::fs::create_dir(path)?;

        let sw = ShardWriter::initialize(Arc::clone(&metadata), tsc, psc, vsc, rsc)?;
        metadata.serialize_metadata()?;
        Ok(sw)
    }

    #[measure(actor = "shard", metric = "open")]
    pub fn open(metadata: Arc<ShardMetadata>) -> NodeResult<ShardWriter> {
        let path = metadata.shard_path();
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel();

        let vsc = VectorConfig {
            similarity: None,
            path: path.join(VECTORS_DIR),
            vectorset: path.join(VECTORSET_DIR),
            channel,
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
            channel,
        };

        ShardWriter::initialize(metadata, tsc, psc, vsc, rsc)
    }

    #[measure(actor = "shard", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    pub fn set_resource(&self, resource: &Resource) -> NodeResult<()> {
        let span = tracing::Span::current();

        let text_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            debug!("Field service starts set_resource");
            let mut writer = write_rw_lock(&text_writer_service);
            let result = writer.set_resource(&field_resource);
            debug!("Field service ends set_resource");
            result
        };

        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            debug!("Paragraph service starts set_resource");
            let mut writer = write_rw_lock(&paragraph_writer_service);
            let result = writer.set_resource(&paragraph_resource);
            debug!("Paragraph service ends set_resource");
            result
        };

        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            debug!("Vector service starts set_resource");
            let mut writer = write_rw_lock(&vector_writer_service);
            let result = writer.set_resource(&vector_resource);
            debug!("Vector service ends set_resource");
            result
        };

        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            debug!("Relation service starts set_resource");
            let mut writer = write_rw_lock(&relation_writer_service);
            let result = writer.set_resource(&relation_resource);
            debug!("Relation service ends set_resource");
            result
        };

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

        let _lock = self.write_lock.lock().expect("Poisoned write lock");
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
        self.metadata.new_generation_id(); // VERY NAIVE, SHOULD BE DONE AFTER MERGE AS WELL
        Ok(())
    }

    #[measure(actor = "shard", metric = "remove_resource")]
    #[tracing::instrument(skip_all)]
    pub fn remove_resource(&self, resource: &ResourceId) -> NodeResult<()> {
        let span = tracing::Span::current();

        let text_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            let mut writer = write_rw_lock(&text_writer_service);
            writer.delete_resource(&field_resource)
        };
        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            let mut writer = write_rw_lock(&paragraph_writer_service);
            writer.delete_resource(&paragraph_resource)
        };
        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            let mut writer = write_rw_lock(&vector_writer_service);
            writer.delete_resource(&vector_resource)
        };
        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            let mut writer = write_rw_lock(&relation_writer_service);
            writer.delete_resource(&relation_resource)
        };

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

        let _lock = self.write_lock.lock().expect("Poisoned write lock");
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

        self.metadata.new_generation_id();

        Ok(())
    }

    #[measure(actor = "shard", metric = "get_opstatus")]
    #[tracing::instrument(skip_all)]
    pub fn get_opstatus(&self) -> NodeResult<OpStatus> {
        let span = tracing::Span::current();

        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let texts = self.text_writer.clone();

        let count_fields =
            || run_with_telemetry(info_span!(parent: &span, "field count"), move || read_rw_lock(&texts).count());
        let count_paragraphs = || {
            run_with_telemetry(info_span!(parent: &span, "paragraph count"), move || read_rw_lock(&paragraphs).count())
        };
        let count_vectors =
            || run_with_telemetry(info_span!(parent: &span, "vector count"), move || read_rw_lock(&vectors).count());

        let mut field_count = Ok(0);
        let mut paragraph_count = Ok(0);
        let mut vector_count = Ok(0);
        thread::scope(|s| {
            s.spawn(|_| field_count = count_fields());
            s.spawn(|_| paragraph_count = count_paragraphs());
            s.spawn(|_| vector_count = count_vectors());
        });

        Ok(OpStatus {
            shard_id: self.id.clone(),
            field_count: field_count? as u64,
            paragraph_count: paragraph_count? as u64,
            sentence_count: vector_count? as u64,
            ..Default::default()
        })
    }

    #[tracing::instrument(skip_all)]
    pub fn list_vectorsets(&self) -> NodeResult<Vec<String>> {
        let reader = read_rw_lock(&self.vector_writer);
        let keys = reader.list_vectorsets()?;
        Ok(keys)
    }

    #[tracing::instrument(skip_all)]
    pub fn add_vectorset(&self, setid: &VectorSetId, similarity: VectorSimilarity) -> NodeResult<()> {
        let mut writer = write_rw_lock(&self.vector_writer);
        writer.add_vectorset(setid, similarity)?;

        self.metadata.new_generation_id();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(&self, setid: &VectorSetId) -> NodeResult<()> {
        let mut writer = write_rw_lock(&self.vector_writer);
        writer.remove_vectorset(setid)?;

        self.metadata.new_generation_id();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn paragraph_count(&self) -> NodeResult<usize> {
        read_rw_lock(&self.paragraph_writer).count()
    }

    #[tracing::instrument(skip_all)]
    pub fn vector_count(&self) -> NodeResult<usize> {
        read_rw_lock(&self.vector_writer).count()
    }

    #[tracing::instrument(skip_all)]
    pub fn text_count(&self) -> NodeResult<usize> {
        read_rw_lock(&self.text_writer).count()
    }

    #[tracing::instrument(skip_all)]
    pub fn collect_garbage(&self) -> NodeResult<GarbageCollectorStatus> {
        let _lock = self.gc_lock.blocking_lock();
        let result = write_rw_lock(&self.vector_writer).garbage_collection();
        match result {
            Ok(()) => Ok(GarbageCollectorStatus::GarbageCollected),
            Err(error) => match error.downcast_ref::<VectorErr>() {
                Some(VectorErr::WorkDelayed) => Ok(GarbageCollectorStatus::TryLater),
                _ => Err(error),
            },
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn force_garbage_collection(&self) -> NodeResult<GarbageCollectorStatus> {
        let _lock = self.gc_lock.blocking_lock();
        let result = write_rw_lock(&self.vector_writer).force_garbage_collection();
        match result {
            Ok(()) => Ok(GarbageCollectorStatus::GarbageCollected),
            Err(error) => match error.downcast_ref::<VectorErr>() {
                Some(VectorErr::WorkDelayed) => Ok(GarbageCollectorStatus::TryLater),
                _ => Err(error),
            },
        }
    }

    #[tracing::instrument(skip_all)]
    pub fn merge(&self, context: MergeContext) -> NodeResult<MergeMetrics> {
        let result = write_rw_lock(&self.vector_writer).merge(context);
        self.metadata.new_generation_id();

        result
    }

    /// This must be used only by replication and should be
    /// deleted as soon as possible.
    #[tracing::instrument(skip_all)]
    pub fn reload(&self) -> NodeResult<()> {
        write_rw_lock(&self.vector_writer).reload()
    }

    pub async fn block_shard(&self) -> BlockingToken {
        let mutex_guard = self.write_lock.lock().expect("Poisoned write lock");
        BlockingToken(mutex_guard)
    }

    pub fn get_shard_segments(&self) -> NodeResult<HashMap<String, Vec<String>>> {
        let mut segments = HashMap::new();

        segments.insert("paragraph".to_string(), read_rw_lock(&self.paragraph_writer).get_segment_ids()?);
        segments.insert("text".to_string(), read_rw_lock(&self.text_writer).get_segment_ids()?);
        segments.insert("vector".to_string(), read_rw_lock(&self.vector_writer).get_segment_ids()?);
        segments.insert("relation".to_string(), read_rw_lock(&self.relation_writer).get_segment_ids()?);

        Ok(segments)
    }

    pub fn get_shard_files(
        &self,
        ignored_segement_ids: &HashMap<String, Vec<String>>,
    ) -> NodeResult<Vec<(PathBuf, IndexFiles)>> {
        let mut files = Vec::new();
        let _lock = self.write_lock.lock().expect("Poisoned write lock"); // need to make sure more writes don't happen while we are reading
        let paragraph_files = read_rw_lock(&self.paragraph_writer)
            .get_index_files(ignored_segement_ids.get("paragraph").unwrap_or(&Vec::new()))?;
        let text_files =
            read_rw_lock(&self.text_writer).get_index_files(ignored_segement_ids.get("text").unwrap_or(&Vec::new()))?;
        let vector_files = read_rw_lock(&self.vector_writer)
            .get_index_files(ignored_segement_ids.get("vector").unwrap_or(&Vec::new()))?;
        let relation_files = read_rw_lock(&self.relation_writer)
            .get_index_files(ignored_segement_ids.get("relation").unwrap_or(&Vec::new()))?;
        files.push((PathBuf::from(PARAGRAPHS_DIR), paragraph_files));
        files.push((PathBuf::from(TEXTS_DIR), text_files));
        files.push((PathBuf::from(VECTORS_DIR), vector_files));
        files.push((PathBuf::from(RELATIONS_DIR), relation_files));
        Ok(files)
    }
}

pub enum GarbageCollectorStatus {
    GarbageCollected,
    TryLater,
}

impl From<GarbageCollectorStatus> for nucliadb_core::protos::garbage_collector_response::Status {
    fn from(value: GarbageCollectorStatus) -> Self {
        match value {
            GarbageCollectorStatus::GarbageCollected => nucliadb_core::protos::garbage_collector_response::Status::Ok,
            GarbageCollectorStatus::TryLater => nucliadb_core::protos::garbage_collector_response::Status::TryLater,
        }
    }
}
