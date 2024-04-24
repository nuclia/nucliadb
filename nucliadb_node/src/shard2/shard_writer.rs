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
use std::sync::{Arc, Mutex, MutexGuard, RwLock};

use nucliadb_core::paragraphs::*;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{DocumentService, ParagraphService, RelationService, VectorService};
use nucliadb_core::protos::{OpStatus, Resource, ResourceId};
use nucliadb_core::relations::*;
use nucliadb_core::texts::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::*;
use nucliadb_core::{thread, IndexFiles};
use nucliadb_procs::measure;
use nucliadb_vectors::VectorErr;

use crate::disk_structure::*;
use crate::shard2::metadata::ShardMetadata;
use crate::shard2::versions::Versions;
use crate::telemetry::run_with_telemetry;

pub struct BlockingToken<'a>(MutexGuard<'a, ()>);

const MAX_LABEL_LENGTH: usize = 32768; // Tantivy max is 2^16 - 4

pub fn open_vectors_writer(version: u32, config: &VectorConfig) -> NodeResult<VectorsWriterPointer> {
    match version {
        1 => nucliadb_vectors::service::VectorWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as VectorsWriterPointer),
        2 => nucliadb_vectors::service::VectorWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as VectorsWriterPointer),
        v => Err(node_error!("Invalid vectors version {v}")),
    }
}
pub fn open_paragraphs_writer(version: u32, config: &ParagraphConfig) -> NodeResult<ParagraphsWriterPointer> {
    match version {
        2 => nucliadb_paragraphs2::writer::ParagraphWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as ParagraphsWriterPointer),
        3 => nucliadb_paragraphs3::writer::ParagraphWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as ParagraphsWriterPointer),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}

pub fn open_texts_writer(version: u32, config: &TextConfig) -> NodeResult<TextsWriterPointer> {
    match version {
        2 => nucliadb_texts2::writer::TextWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as TextsWriterPointer),
        v => Err(node_error!("Invalid text writer version {v}")),
    }
}

pub fn open_relations_writer(version: u32, config: &RelationConfig) -> NodeResult<RelationsWriterPointer> {
    match version {
        2 => nucliadb_relations2::writer::RelationsWriterService::start(config)
            .map(|i| Arc::new(RwLock::new(i)) as RelationsWriterPointer),
        v => Err(node_error!("Invalid relations version {v}")),
    }
}

fn remove_invalid_labels(resource: &mut Resource) {
    resource.labels.retain(|l| {
        if l.len() > MAX_LABEL_LENGTH {
            warn!("Label length ({}) longer than maximum, it will not be indexed", l.len());
            false
        } else {
            true
        }
    });
    for text in resource.texts.values_mut() {
        text.labels.retain(|l| {
            if l.len() > MAX_LABEL_LENGTH {
                warn!("Label length ({}) longer than maximum, it will not be indexed", l.len());
                false
            } else {
                true
            }
        });
    }
}

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
        let versions_path = metadata.shard_path().join(VERSION_FILE);
        let versions = Versions::create(&versions_path, metadata.channel())?;
        let text_task = || Some(open_texts_writer(versions.texts, &tsc));
        let paragraph_task = || Some(open_paragraphs_writer(versions.paragraphs, &psc));
        let vector_task = || Some(open_vectors_writer(versions.vectors, &vsc));
        let relation_task = || Some(open_relations_writer(versions.relations, &rsc));

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
            document_service_version: versions.texts as i32,
            paragraph_service_version: versions.paragraphs as i32,
            vector_service_version: versions.vectors as i32,
            relation_service_version: versions.relations as i32,
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
            3 => ParagraphService::ParagraphV3,
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
            channel,
            shard_id: metadata.id(),
            normalize_vectors: metadata.normalize_vectors(),
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
            channel,
            shard_id: metadata.id(),
            normalize_vectors: metadata.normalize_vectors(),
        };
        let rsc = RelationConfig {
            path: path.join(RELATIONS_DIR),
            channel,
        };

        ShardWriter::initialize(metadata, tsc, psc, vsc, rsc)
    }

    #[measure(actor = "shard", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    pub fn set_resource(&self, mut resource: Resource) -> NodeResult<()> {
        let span = tracing::Span::current();

        remove_invalid_labels(&mut resource);

        let text_task = || {
            debug!("Field service starts set_resource");
            let mut writer = write_rw_lock(&self.text_writer);
            let result = writer.set_resource(&resource);
            debug!("Field service ends set_resource");
            result
        };

        let paragraph_task = || {
            debug!("Paragraph service starts set_resource");
            let mut writer = write_rw_lock(&self.paragraph_writer);
            let result = writer.set_resource(&resource);
            debug!("Paragraph service ends set_resource");
            result
        };

        let vector_task = || {
            debug!("Vector service starts set_resource");
            let mut writer = write_rw_lock(&self.vector_writer);
            let result = writer.set_resource(&resource);
            debug!("Vector service ends set_resource");
            result
        };

        let relation_task = || {
            debug!("Relation service starts set_resource");
            let mut writer = write_rw_lock(&self.relation_writer);
            let result = writer.set_resource(&resource);
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

        let _lock = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
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

        let text_task = || {
            let mut writer = write_rw_lock(&self.text_writer);
            writer.delete_resource(resource)
        };

        let paragraph_task = || {
            let mut writer = write_rw_lock(&self.paragraph_writer);
            writer.delete_resource(resource)
        };

        let vector_task = || {
            let mut writer = write_rw_lock(&self.vector_writer);
            writer.delete_resource(resource)
        };

        let relation_task = move || {
            let mut writer = write_rw_lock(&self.relation_writer);
            writer.delete_resource(resource)
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

        let _lock = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
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

        let count_fields = || {
            run_with_telemetry(info_span!(parent: &span, "field count"), move || {
                read_rw_lock(&self.text_writer).count()
            })
        };
        let count_paragraphs = || {
            run_with_telemetry(info_span!(parent: &span, "paragraph count"), move || {
                read_rw_lock(&self.paragraph_writer).count()
            })
        };
        let count_vectors = || {
            run_with_telemetry(info_span!(parent: &span, "vector count"), move || {
                read_rw_lock(&self.vector_writer).count()
            })
        };

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
        let runner = read_rw_lock(&self.vector_writer).prepare_merge(context.parameters)?;
        let Some(mut runner) = runner else {
            return Ok(MergeMetrics {
                merged: 0,
                left: 0,
            });
        };
        let merge_result = runner.run()?;
        let metrics = write_rw_lock(&self.vector_writer).record_merge(merge_result, context.source)?;
        self.metadata.new_generation_id();

        Ok(metrics)
    }

    /// This must be used only by replication and should be
    /// deleted as soon as possible.
    #[tracing::instrument(skip_all)]
    pub fn reload(&self) -> NodeResult<()> {
        write_rw_lock(&self.vector_writer).reload()
    }

    pub async fn block_shard(&self) -> BlockingToken {
        let mutex_guard = self.write_lock.lock().unwrap_or_else(|e| e.into_inner());
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
        let _lock = self.write_lock.lock().unwrap_or_else(|e| e.into_inner()); // need to make sure more writes don't happen while we are reading
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