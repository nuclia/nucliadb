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
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use nucliadb_core::paragraphs::*;
use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{DocumentService, ParagraphService, RelationService, VectorService};
use nucliadb_core::protos::{Resource, ResourceId};
use nucliadb_core::relations::*;
use nucliadb_core::texts::*;
use nucliadb_core::tracing::{self, *};
use nucliadb_core::vectors::*;
use nucliadb_core::{thread, IndexFiles};
use nucliadb_procs::measure;
use nucliadb_vectors::VectorErr;

use super::indexes::ShardIndexes;
use super::metadata::ShardMetadata;
use super::versioning::{self, Versions};
use crate::disk_structure::*;
use crate::telemetry::run_with_telemetry;

const MAX_LABEL_LENGTH: usize = 32768; // Tantivy max is 2^16 - 4

pub fn open_vectors_writer(version: u32, path: &Path, shard_id: String) -> NodeResult<VectorsWriterPointer> {
    match version {
        1 => nucliadb_vectors::service::VectorWriterService::open(path, shard_id)
            .map(|i| Box::new(i) as VectorsWriterPointer),
        2 => nucliadb_vectors::service::VectorWriterService::open(path, shard_id)
            .map(|i| Box::new(i) as VectorsWriterPointer),
        v => Err(node_error!("Invalid vectors version {v}")),
    }
}
pub fn open_paragraphs_writer(version: u32, config: &ParagraphConfig) -> NodeResult<ParagraphsWriterPointer> {
    match version {
        2 => nucliadb_paragraphs2::writer::ParagraphWriterService::open(config)
            .map(|i| Box::new(i) as ParagraphsWriterPointer),
        3 => nucliadb_paragraphs3::writer::ParagraphWriterService::open(config)
            .map(|i| Box::new(i) as ParagraphsWriterPointer),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}

pub fn open_texts_writer(version: u32, config: &TextConfig) -> NodeResult<TextsWriterPointer> {
    match version {
        2 => nucliadb_texts2::writer::TextWriterService::open(config).map(|i| Box::new(i) as TextsWriterPointer),
        v => Err(node_error!("Invalid text writer version {v}")),
    }
}

pub fn open_relations_writer(version: u32, config: &RelationConfig) -> NodeResult<RelationsWriterPointer> {
    match version {
        2 => nucliadb_relations2::writer::RelationsWriterService::open(config)
            .map(|i| Box::new(i) as RelationsWriterPointer),
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
    indexes: RwLock<InnerShardWriter>,
    versions: Versions,
    pub gc_lock: tokio::sync::Mutex<()>, // lock to be able to do GC or not
}

#[derive(Debug)]
struct InnerShardWriter {
    text_writer: TextsWriterPointer,
    paragraph_writer: ParagraphsWriterPointer,
    vector_writer: VectorsWriterPointer,
    relation_writer: RelationsWriterPointer,
}

impl ShardWriter {
    #[tracing::instrument(skip_all)]
    pub fn document_version(&self) -> DocumentService {
        match self.versions.texts {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            2 => DocumentService::DocumentV2,
            i => panic!("Unknown document version {i}"),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn paragraph_version(&self) -> ParagraphService {
        match self.versions.paragraphs {
            0 => ParagraphService::ParagraphV0,
            1 => ParagraphService::ParagraphV1,
            2 => ParagraphService::ParagraphV2,
            3 => ParagraphService::ParagraphV3,
            i => panic!("Unknown paragraph version {i}"),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn vector_version(&self) -> VectorService {
        match self.versions.vectors {
            0 => VectorService::VectorV0,
            1 => VectorService::VectorV1,
            i => panic!("Unknown vector version {i}"),
        }
    }
    #[tracing::instrument(skip_all)]
    pub fn relation_version(&self) -> RelationService {
        match self.versions.relations {
            0 => RelationService::RelationV0,
            1 => RelationService::RelationV1,
            2 => RelationService::RelationV2,
            i => panic!("Unknown relation version {i}"),
        }
    }

    #[measure(actor = "shard", metric = "new")]
    pub fn new(metadata: Arc<ShardMetadata>) -> NodeResult<ShardWriter> {
        let shard_path = metadata.shard_path();
        let indexes = ShardIndexes::new(&shard_path);

        let tsc = TextConfig {
            path: indexes.texts_path(),
        };
        let psc = ParagraphConfig {
            path: indexes.paragraphs_path(),
        };
        let vsc = VectorConfig {
            similarity: metadata.similarity(),
            path: indexes.vectors_path(),
            channel: metadata.channel(),
            shard_id: metadata.id(),
            normalize_vectors: metadata.normalize_vectors(),
        };
        let rsc = RelationConfig {
            path: indexes.relations_path(),
            channel: metadata.channel(),
        };

        std::fs::create_dir(shard_path)?;

        let versions = Versions {
            paragraphs: versioning::PARAGRAPHS_VERSION,
            vectors: versioning::VECTORS_VERSION,
            texts: versioning::TEXTS_VERSION,
            relations: versioning::RELATIONS_VERSION,
        };
        let versions_path = metadata.shard_path().join(VERSION_FILE);
        Versions::create(&versions_path, versions)?;

        let text_task = || Some(nucliadb_texts2::writer::TextWriterService::create(tsc));
        let paragraph_task = || Some(nucliadb_paragraphs3::writer::ParagraphWriterService::create(psc));
        let vector_task = || Some(nucliadb_vectors::service::VectorWriterService::create(vsc));
        let relation_task = || Some(nucliadb_relations2::writer::RelationsWriterService::create(rsc));

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

        metadata.serialize_metadata()?;
        indexes.store()?;

        Ok(ShardWriter {
            id: metadata.id(),
            path: metadata.shard_path(),
            metadata,
            indexes: RwLock::new(InnerShardWriter {
                text_writer: Box::new(fields.unwrap()),
                paragraph_writer: Box::new(paragraphs.unwrap()),
                vector_writer: Box::new(vectors.unwrap()),
                relation_writer: Box::new(relations.unwrap()),
            }),
            versions,
            gc_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[measure(actor = "shard", metric = "open")]
    pub fn open(metadata: Arc<ShardMetadata>) -> NodeResult<ShardWriter> {
        let shard_path = metadata.shard_path();
        let indexes = ShardIndexes::load(&shard_path).unwrap_or_else(|_| ShardIndexes::new(&shard_path));

        // TODO: this call will generate the shard indexes file, as a lazy
        // migration. When every shard has the file, this line should be
        // removed
        indexes.store()?;

        let tsc = TextConfig {
            path: indexes.texts_path(),
        };
        let psc = ParagraphConfig {
            path: indexes.paragraphs_path(),
        };
        let rsc = RelationConfig {
            path: indexes.relations_path(),
            channel: metadata.channel(),
        };

        let versions_path = metadata.shard_path().join(VERSION_FILE);
        let versions = Versions::load(&versions_path)?;

        let text_task = || Some(open_texts_writer(versions.texts, &tsc));
        let paragraph_task = || Some(open_paragraphs_writer(versions.paragraphs, &psc));
        let vector_task = || Some(open_vectors_writer(versions.vectors, &indexes.vectors_path(), metadata.id()));
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
            indexes: RwLock::new(InnerShardWriter {
                text_writer: fields.unwrap(),
                paragraph_writer: paragraphs.unwrap(),
                vector_writer: vectors.unwrap(),
                relation_writer: relations.unwrap(),
            }),
            versions,
            gc_lock: tokio::sync::Mutex::new(()),
        })
    }

    #[measure(actor = "shard", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    pub fn set_resource(&self, mut resource: Resource) -> NodeResult<()> {
        let span = tracing::Span::current();

        remove_invalid_labels(&mut resource);

        let indexes: &mut InnerShardWriter = &mut write_rw_lock(&self.indexes);

        let text_task = || {
            debug!("Field service starts set_resource");
            let result = indexes.text_writer.set_resource(&resource);
            debug!("Field service ends set_resource");
            result
        };

        let paragraph_task = || {
            debug!("Paragraph service starts set_resource");
            let result = indexes.paragraph_writer.set_resource(&resource);
            debug!("Paragraph service ends set_resource");
            result
        };

        let vector_task = || {
            debug!("Vector service starts set_resource");
            let result = indexes.vector_writer.set_resource(&resource);
            debug!("Vector service ends set_resource");
            result
        };

        let relation_task = || {
            debug!("Relation service starts set_resource");
            let result = indexes.relation_writer.set_resource(&resource);
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

        let indexes: &mut InnerShardWriter = &mut write_rw_lock(&self.indexes);

        let text_task = || indexes.text_writer.delete_resource(resource);

        let paragraph_task = || indexes.paragraph_writer.delete_resource(resource);

        let vector_task = || indexes.vector_writer.delete_resource(resource);

        let relation_task = || indexes.relation_writer.delete_resource(resource);

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

        self.metadata.new_generation_id();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn collect_garbage(&self) -> NodeResult<GarbageCollectorStatus> {
        let _lock = self.gc_lock.blocking_lock();
        let result = write_rw_lock(&self.indexes).vector_writer.garbage_collection();
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
        let result = write_rw_lock(&self.indexes).vector_writer.force_garbage_collection();
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
        let runner = read_rw_lock(&self.indexes).vector_writer.prepare_merge(context.parameters)?;
        let Some(mut runner) = runner else {
            return Ok(MergeMetrics {
                merged: 0,
                left: 0,
            });
        };
        let merge_result = runner.run()?;
        let metrics = write_rw_lock(&self.indexes).vector_writer.record_merge(merge_result, context.source)?;
        self.metadata.new_generation_id();

        Ok(metrics)
    }

    /// This must be used only by replication and should be
    /// deleted as soon as possible.
    #[tracing::instrument(skip_all)]
    pub fn reload(&self) -> NodeResult<()> {
        write_rw_lock(&self.indexes).vector_writer.reload()
    }

    pub fn get_shard_segments(&self) -> NodeResult<HashMap<String, Vec<String>>> {
        let mut segments = HashMap::new();

        segments.insert("paragraph".to_string(), read_rw_lock(&self.indexes).paragraph_writer.get_segment_ids()?);
        segments.insert("text".to_string(), read_rw_lock(&self.indexes).text_writer.get_segment_ids()?);
        segments.insert("vector".to_string(), read_rw_lock(&self.indexes).vector_writer.get_segment_ids()?);
        segments.insert("relation".to_string(), read_rw_lock(&self.indexes).relation_writer.get_segment_ids()?);

        Ok(segments)
    }

    pub fn get_shard_files(
        &self,
        ignored_segement_ids: &HashMap<String, Vec<String>>,
    ) -> NodeResult<Vec<(PathBuf, IndexFiles)>> {
        let mut files = Vec::new();
        // we get a write lock here to block any possible write on the indexes
        // while we retrieve the list of files
        let indexes = write_rw_lock(&self.indexes);
        let paragraph_files =
            indexes.paragraph_writer.get_index_files(ignored_segement_ids.get("paragraph").unwrap_or(&Vec::new()))?;
        let text_files =
            indexes.text_writer.get_index_files(ignored_segement_ids.get("text").unwrap_or(&Vec::new()))?;
        let vector_files =
            indexes.vector_writer.get_index_files(ignored_segement_ids.get("vector").unwrap_or(&Vec::new()))?;
        let relation_files =
            indexes.relation_writer.get_index_files(ignored_segement_ids.get("relation").unwrap_or(&Vec::new()))?;
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
