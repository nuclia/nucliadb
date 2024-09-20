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
use nucliadb_vectors::config::VectorConfig;
use nucliadb_vectors::VectorErr;

use super::indexes::{ShardIndexes, DEFAULT_VECTORS_INDEX_NAME};
use super::metadata::ShardMetadata;
use super::versioning::{self, Versions};
use crate::disk_structure::{self, *};
use crate::settings::{feature_flags, Settings};
use crate::telemetry::run_with_telemetry;

const MAX_LABEL_LENGTH: usize = 32768; // Tantivy max is 2^16 - 4

#[derive(Debug)]
pub struct ShardWriter {
    pub metadata: Arc<ShardMetadata>,
    pub id: String,
    pub path: PathBuf,
    indexes: RwLock<ShardWriterIndexes>,
    versions: Versions,
    pub gc_lock: tokio::sync::Mutex<()>, // lock to be able to do GC or not
}

#[derive(Debug)]
struct ShardWriterIndexes {
    texts_index: TextsWriterPointer,
    paragraphs_index: ParagraphsWriterPointer,
    vectors_indexes: HashMap<String, VectorsWriterPointer>,
    relations_index: RelationsWriterPointer,
}

pub struct NewShard {
    pub kbid: String,
    pub shard_id: String,
    pub vector_configs: HashMap<String, VectorConfig>,
}

impl ShardWriter {
    #[tracing::instrument(skip_all)]
    pub fn document_version(&self) -> DocumentService {
        match self.versions.texts {
            0 => DocumentService::DocumentV0,
            1 => DocumentService::DocumentV1,
            2 => DocumentService::DocumentV2,
            3 => DocumentService::DocumentV3,
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
    pub fn new(new: NewShard, shards_path: &Path, settings: &Settings) -> NodeResult<(Self, Arc<ShardMetadata>)> {
        let span = tracing::Span::current();

        if new.vector_configs.is_empty() {
            return Err(node_error!("Shards must be created with at least one vector index"));
        }

        let shard_id = new.shard_id;
        let shard_path = disk_structure::shard_path_by_id(shards_path, &shard_id);
        let metadata = Arc::new(ShardMetadata::new(shard_path.clone(), shard_id.clone(), new.kbid));
        let mut indexes = ShardIndexes::new(&shard_path);

        std::fs::create_dir(&shard_path)?;

        let ff_context = HashMap::from([("kbid".to_string(), metadata.kbid())]);
        let texts3_enabled = settings.has_feature(feature_flags::TEXTS3, ff_context);
        let texts_version = if texts3_enabled {
            3
        } else {
            2
        };

        let versions = Versions {
            paragraphs: versioning::PARAGRAPHS_VERSION,
            vectors: versioning::VECTORS_VERSION,
            texts: versioning::TEXTS_VERSION,
            relations: versioning::RELATIONS_VERSION,
        };
        let versions_path = shard_path.join(VERSION_FILE);
        Versions::create(&versions_path, versions)?;

        // indexes creation tasks

        let tsc = TextConfig {
            path: indexes.texts_path(),
        };
        let text_task = || Some(create_texts_writer(texts_version, tsc));
        let info = info_span!(parent: &span, "text start");
        let text_task = || run_with_telemetry(info, text_task);

        let psc = ParagraphConfig {
            path: indexes.paragraphs_path(),
        };
        let paragraph_task = || Some(nucliadb_paragraphs3::writer::ParagraphWriterService::create(psc));
        let info = info_span!(parent: &span, "paragraph start");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);

        let mut vector_tasks = vec![];
        for (vectorset_id, config) in new.vector_configs {
            let vectorset_path = indexes.add_vectors_index(vectorset_id.clone())?;
            let shard_id_clone = shard_id.clone();
            vector_tasks.push(|| {
                run_with_telemetry(info_span!(parent: &span, "vector start"), move || {
                    Some((
                        vectorset_id,
                        nucliadb_vectors::service::VectorWriterService::create(&vectorset_path, shard_id_clone, config),
                    ))
                })
            })
        }

        let rsc = RelationConfig {
            path: indexes.relations_path(),
        };
        let relation_task = || Some(nucliadb_relations2::writer::RelationsWriterService::create(rsc));
        let info = info_span!(parent: &span, "relation start");
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = None;
        let mut paragraph_result = None;
        let mut vector_results = Vec::with_capacity(vector_tasks.len());
        for _ in 0..vector_tasks.len() {
            vector_results.push(None);
        }
        let mut relation_result = None;
        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            for (vector_task, vector_result) in vector_tasks.into_iter().zip(vector_results.iter_mut()) {
                s.spawn(|_| *vector_result = vector_task());
            }
            s.spawn(|_| relation_result = relation_task());
        });

        let fields = text_result.transpose()?;
        let paragraphs = paragraph_result.transpose()?;
        let mut vectors = HashMap::with_capacity(vector_results.len());
        for result in vector_results {
            let (name, vector_writer) = result.unwrap();
            vectors.insert(name, Box::new(vector_writer?) as VectorsWriterPointer);
        }
        let relations = relation_result.transpose()?;

        metadata.serialize_metadata()?;
        indexes.store()?;

        Ok((
            ShardWriter {
                id: shard_id,
                path: shard_path,
                metadata: Arc::clone(&metadata),
                indexes: RwLock::new(ShardWriterIndexes {
                    texts_index: fields.unwrap(),
                    paragraphs_index: Box::new(paragraphs.unwrap()),
                    vectors_indexes: vectors,
                    relations_index: Box::new(relations.unwrap()),
                }),
                versions,
                gc_lock: tokio::sync::Mutex::new(()),
            },
            metadata,
        ))
    }

    #[measure(actor = "shard", metric = "open")]
    pub fn open(metadata: Arc<ShardMetadata>) -> NodeResult<ShardWriter> {
        let span = tracing::Span::current();
        let shard_path = metadata.shard_path();

        // fallback to default indexes while there are shards without the file
        let indexes = ShardIndexes::load(&shard_path).or_else(|_| {
            let mut indexes = ShardIndexes::new(&shard_path);
            indexes.add_vectors_index(DEFAULT_VECTORS_INDEX_NAME.to_string())?;
            Ok::<ShardIndexes, anyhow::Error>(indexes)
        })?;

        // This call will generate the shard indexes file, as a lazy migration.
        // TODO: When every shard has the file, this line should be removed.
        // Currently, all hosted environments are migrated, missing validation
        // for onprem
        indexes.store()?;

        let versions_path = metadata.shard_path().join(VERSION_FILE);
        let versions = Versions::load(&versions_path)?;

        let tsc = TextConfig {
            path: indexes.texts_path(),
        };
        let text_task = || Some(open_texts_writer(versions.texts, &tsc));
        let info = info_span!(parent: &span, "Open texts index writer");
        let text_task = || run_with_telemetry(info, text_task);

        let psc = ParagraphConfig {
            path: indexes.paragraphs_path(),
        };
        let paragraph_task = || Some(open_paragraphs_writer(versions.paragraphs, &psc));
        let info = info_span!(parent: &span, "Open paragraphs index writer");
        let paragraph_task = || run_with_telemetry(info, paragraph_task);

        let mut vector_tasks = vec![];
        for (name, path) in indexes.iter_vectors_indexes() {
            let id = metadata.id();
            vector_tasks.push(|| {
                run_with_telemetry(info_span!(parent: &span, "Open vectors index writer"), move || {
                    Some((name, open_vectors_writer(versions.vectors, &path, id)))
                })
            });
        }

        let rsc = RelationConfig {
            path: indexes.relations_path(),
        };
        let info = info_span!(parent: &span, "Open relations index writer");
        let relation_task = || Some(open_relations_writer(versions.relations, &rsc));
        let relation_task = || run_with_telemetry(info, relation_task);

        let mut text_result = None;
        let mut paragraph_result = None;
        let mut vector_results = Vec::with_capacity(vector_tasks.len());
        for _ in 0..vector_tasks.len() {
            vector_results.push(None);
        }
        let mut relation_result = None;
        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            for (vector_task, vector_result) in vector_tasks.into_iter().zip(vector_results.iter_mut()) {
                s.spawn(|_| *vector_result = vector_task());
            }
            s.spawn(|_| relation_result = relation_task());
        });

        let texts = text_result.unwrap()?;
        let paragraphs = paragraph_result.unwrap()?;
        let mut vectors = HashMap::with_capacity(vector_results.len());
        for result in vector_results {
            let (name, vector_writer) = result.unwrap();
            vectors.insert(name, vector_writer?);
        }
        let relations = relation_result.unwrap()?;

        Ok(ShardWriter {
            id: metadata.id(),
            path: metadata.shard_path(),
            metadata,
            indexes: RwLock::new(ShardWriterIndexes {
                texts_index: texts,
                paragraphs_index: paragraphs,
                vectors_indexes: vectors,
                relations_index: relations,
            }),
            versions,
            gc_lock: tokio::sync::Mutex::new(()),
        })
    }

    pub fn create_vectors_index(&self, name: String, new: VectorConfig) -> NodeResult<()> {
        let mut indexes = ShardIndexes::load(&self.metadata.shard_path())?;
        let path = indexes.add_vectors_index(name.clone())?;
        let vectors_writer = nucliadb_vectors::service::VectorWriterService::create(&path, self.id.clone(), new)?;
        indexes.store()?;
        write_rw_lock(&self.indexes).vectors_indexes.insert(name, Box::new(vectors_writer));
        Ok(())
    }

    pub fn remove_vectors_index(&self, name: String) -> NodeResult<()> {
        let mut indexes = ShardIndexes::load(&self.metadata.shard_path())?;
        let path = indexes.remove_vectors_index(&name)?;
        indexes.store()?;
        write_rw_lock(&self.indexes).vectors_indexes.remove(&name);
        if let Some(path) = path {
            // Although there can be a reader with this index open, readers
            // currently open all vectors index files so we rely on Linux not
            // deleting the files until it closes the index. Readers then should
            // be able to keep answering for that vectorset until closing it
            std::fs::remove_dir_all(path)?;
        }
        Ok(())
    }

    pub fn list_vectors_indexes(&self) -> Vec<String> {
        read_rw_lock(&self.indexes).vectors_indexes.keys().cloned().collect::<Vec<String>>()
    }

    #[measure(actor = "shard", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    pub fn set_resource(&self, mut resource: Resource) -> NodeResult<()> {
        let span = tracing::Span::current();

        remove_invalid_labels(&mut resource);

        let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);

        let mut text_task = || {
            run_with_telemetry(info_span!(parent: &span, "text set_resource"), || {
                debug!("Field service starts set_resource");
                let result = indexes.texts_index.set_resource(&resource);
                debug!("Field service ends set_resource");
                result
            })
        };

        let mut paragraph_task = || {
            run_with_telemetry(info_span!(parent: &span, "paragraph set_resource"), || {
                debug!("Paragraph service starts set_resource");
                let result = indexes.paragraphs_index.set_resource(&resource);
                debug!("Paragraph service ends set_resource");
                result
            })
        };

        let mut vector_tasks = vec![];
        let vectorset_count = indexes.vectors_indexes.len();
        for (vectorset, vector_writer) in indexes.vectors_indexes.iter_mut() {
            vector_tasks.push(|| {
                run_with_telemetry(info_span!(parent: &span, "vector set_resource"), || {
                    debug!("Vector service starts set_resource");

                    let vectorset_resource = match vectorset.as_str() {
                        "" | DEFAULT_VECTORS_INDEX_NAME => (&resource).into(),
                        vectorset => nucliadb_core::vectors::ResourceWrapper::new_vectorset_resource(
                            &resource,
                            vectorset,
                            vectorset_count == 1,
                        ),
                    };
                    let result = vector_writer.set_resource(vectorset_resource);
                    debug!("Vector service ends set_resource");
                    result
                })
            });
        }

        let mut relation_task = || {
            run_with_telemetry(info_span!(parent: &span, "relation set_resource"), || {
                debug!("Relation service starts set_resource");
                let result = indexes.relations_index.set_resource(&resource);
                debug!("Relation service ends set_resource");
                result
            })
        };

        let mut text_result = Ok(());
        let mut paragraph_result = Ok(());
        let mut vector_results = Vec::with_capacity(vector_tasks.len());
        for _ in 0..vector_tasks.len() {
            vector_results.push(Ok(()));
        }
        let mut relation_result = Ok(());

        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            for (mut vector_task, vector_result) in vector_tasks.into_iter().zip(vector_results.iter_mut()) {
                s.spawn(move |_| *vector_result = vector_task());
            }
            s.spawn(|_| relation_result = relation_task());
        });

        text_result?;
        paragraph_result?;
        for result in vector_results {
            result?
        }
        relation_result?;
        self.metadata.new_generation_id(); // VERY NAIVE, SHOULD BE DONE AFTER MERGE AS WELL

        Ok(())
    }

    #[measure(actor = "shard", metric = "remove_resource")]
    #[tracing::instrument(skip_all)]
    pub fn remove_resource(&self, resource: &ResourceId) -> NodeResult<()> {
        let span = tracing::Span::current();

        let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);

        let mut text_task = || {
            run_with_telemetry(info_span!(parent: &span, "text remove"), || {
                indexes.texts_index.delete_resource(resource)
            })
        };

        let mut paragraph_task = || {
            run_with_telemetry(info_span!(parent: &span, "paragraph remove"), || {
                indexes.paragraphs_index.delete_resource(resource)
            })
        };

        let mut vector_tasks = vec![];
        for (_, vector_writer) in indexes.vectors_indexes.iter_mut() {
            vector_tasks.push(|| {
                run_with_telemetry(info_span!(parent: &span, "vector remove"), || {
                    vector_writer.delete_resource(resource)
                })
            });
        }

        let mut relation_task = || {
            run_with_telemetry(info_span!(parent: &span, "relation remove"), || {
                indexes.relations_index.delete_resource(resource)
            })
        };

        let mut text_result = Ok(());
        let mut paragraph_result = Ok(());
        let mut vector_results = Vec::with_capacity(vector_tasks.len());
        for _ in 0..vector_tasks.len() {
            vector_results.push(Ok(()));
        }
        let mut relation_result = Ok(());

        thread::scope(|s| {
            s.spawn(|_| text_result = text_task());
            s.spawn(|_| paragraph_result = paragraph_task());
            for (mut vector_task, vector_result) in vector_tasks.into_iter().zip(vector_results.iter_mut()) {
                s.spawn(move |_| *vector_result = vector_task());
            }
            s.spawn(|_| relation_result = relation_task());
        });

        text_result?;
        paragraph_result?;
        for result in vector_results {
            result?
        }
        relation_result?;

        self.metadata.new_generation_id();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn collect_garbage(&self) -> NodeResult<GarbageCollectorStatus> {
        let _lock = self.gc_lock.blocking_lock();
        let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);

        let mut gc_results = Vec::with_capacity(indexes.vectors_indexes.len());
        for (_, vector_writer) in indexes.vectors_indexes.iter_mut() {
            let result = vector_writer.garbage_collection();
            gc_results.push(result);
        }

        for result in gc_results {
            if let Err(error) = result {
                return match error.downcast_ref::<VectorErr>() {
                    Some(VectorErr::WorkDelayed) => Ok(GarbageCollectorStatus::TryLater),
                    _ => Err(error),
                };
            }
        }
        Ok(GarbageCollectorStatus::GarbageCollected)
    }

    #[tracing::instrument(skip_all)]
    pub fn force_garbage_collection(&self) -> NodeResult<GarbageCollectorStatus> {
        let _lock = self.gc_lock.blocking_lock();
        let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);

        let mut gc_results = Vec::with_capacity(indexes.vectors_indexes.len());
        for (_, vector_writer) in indexes.vectors_indexes.iter_mut() {
            let result = vector_writer.force_garbage_collection();
            gc_results.push(result);
        }

        for result in gc_results {
            if let Err(error) = result {
                return match error.downcast_ref::<VectorErr>() {
                    Some(VectorErr::WorkDelayed) => Ok(GarbageCollectorStatus::TryLater),
                    _ => Err(error),
                };
            }
        }
        Ok(GarbageCollectorStatus::GarbageCollected)
    }

    #[tracing::instrument(skip_all)]
    pub fn merge(&self, context: MergeContext) -> NodeResult<MergeMetrics> {
        let mut runners = HashMap::new();
        {
            let indexes: &ShardWriterIndexes = &read_rw_lock(&self.indexes);
            for (name, vectors_index) in indexes.vectors_indexes.iter() {
                let runner = vectors_index.prepare_merge(context.parameters);
                if let Ok(Some(runner)) = runner {
                    runners.insert(name.clone(), runner);
                }
            }
        }

        // Running merge is costly, so we don't want a lock indexes while merging
        let mut merge_results = HashMap::new();
        for (name, mut runner) in runners.into_iter() {
            let result = runner.run();
            merge_results.insert(name, result);
        }

        {
            let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);

            let mut metrics = MergeMetrics {
                merged: 0,
                left: 0,
            };
            let mut prepare_merge_errors = Vec::new();
            let mut record_merge_errors = Vec::new();
            for (name, vectors_index) in indexes.vectors_indexes.iter_mut() {
                let result = merge_results.remove(name);
                if result.is_none() {
                    // new index have been added while acquiring indexes a
                    // second time. Ignore
                    continue;
                }
                let result = result.unwrap();

                if let Ok(merge_result) = result {
                    let recorded = vectors_index.record_merge(merge_result, context.source);
                    if let Ok(m) = recorded {
                        metrics.merged += m.merged;
                        metrics.left += m.left;
                    } else {
                        record_merge_errors.push(recorded);
                    }
                } else {
                    prepare_merge_errors.push(result);
                }
            }

            // return one of the errors we found if there are
            for error in prepare_merge_errors {
                error?;
            }
            for error in record_merge_errors {
                error?;
            }

            Ok(metrics)
        }
    }

    /// This must be used only by replication and should be
    /// deleted as soon as possible.
    #[tracing::instrument(skip_all)]
    pub fn reload(&self) -> NodeResult<()> {
        let indexes: &mut ShardWriterIndexes = &mut write_rw_lock(&self.indexes);
        let mut results = Vec::with_capacity(indexes.vectors_indexes.len());
        for (_, vector_writer) in indexes.vectors_indexes.iter_mut() {
            results.push(vector_writer.reload());
        }
        for result in results {
            result?;
        }
        Ok(())
    }

    pub fn get_shard_segments(&self) -> NodeResult<HashMap<String, Vec<String>>> {
        let mut segments = HashMap::new();
        let indexes: &ShardWriterIndexes = &read_rw_lock(&self.indexes);

        segments.insert("paragraph".to_string(), indexes.paragraphs_index.get_segment_ids()?);
        segments.insert("text".to_string(), indexes.texts_index.get_segment_ids()?);
        // TODO: return segments for all vector indexes
        let default_vectors_index = indexes
            .vectors_indexes
            .get(DEFAULT_VECTORS_INDEX_NAME)
            .expect("Default vectors index should never be deleted (yet)");
        segments.insert("vector".to_string(), default_vectors_index.get_segment_ids()?);
        segments.insert("relation".to_string(), indexes.relations_index.get_segment_ids()?);

        Ok(segments)
    }

    pub fn get_shard_files(
        &self,
        ignored_segment_ids: &HashMap<String, Vec<String>>,
    ) -> NodeResult<Vec<(PathBuf, IndexFiles)>> {
        let mut files = Vec::new();
        // we get a write lock here to block any possible write on the indexes
        // while we retrieve the list of files
        let indexes = write_rw_lock(&self.indexes);
        let paragraph_files =
            indexes.paragraphs_index.get_index_files(ignored_segment_ids.get("paragraph").unwrap_or(&Vec::new()))?;
        let text_files = indexes.texts_index.get_index_files(ignored_segment_ids.get("text").unwrap_or(&Vec::new()))?;

        // Vector indexes
        let index_meta = ShardIndexes::load(&self.path)?;
        for (key, index) in &indexes.vectors_indexes {
            if let Some(path) = index_meta.vectorset_relative_path(key) {
                files.push((
                    path.clone(),
                    index.get_index_files(
                        path.to_str().unwrap(),
                        ignored_segment_ids.get("vector").unwrap_or(&Vec::new()),
                    )?,
                ));
            }
        }
        let relation_files =
            indexes.relations_index.get_index_files(ignored_segment_ids.get("relation").unwrap_or(&Vec::new()))?;
        files.push((PathBuf::from(PARAGRAPHS_DIR), paragraph_files));
        files.push((PathBuf::from(TEXTS_DIR), text_files));
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
        3 => nucliadb_paragraphs3::writer::ParagraphWriterService::open(config)
            .map(|i| Box::new(i) as ParagraphsWriterPointer),
        v => Err(node_error!("Invalid paragraphs version {v}")),
    }
}

pub fn open_texts_writer(version: u32, config: &TextConfig) -> NodeResult<TextsWriterPointer> {
    match version {
        2 => nucliadb_texts2::writer::TextWriterService::open(config).map(|i| Box::new(i) as TextsWriterPointer),
        3 => nucliadb_texts3::writer::TextWriterService::open(config).map(|i| Box::new(i) as TextsWriterPointer),
        v => Err(node_error!("Invalid text writer version {v}")),
    }
}

pub fn create_texts_writer(version: u32, config: TextConfig) -> NodeResult<TextsWriterPointer> {
    match version {
        2 => nucliadb_texts2::writer::TextWriterService::create(config).map(|i| Box::new(i) as TextsWriterPointer),
        3 => nucliadb_texts3::writer::TextWriterService::create(config).map(|i| Box::new(i) as TextsWriterPointer),
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
