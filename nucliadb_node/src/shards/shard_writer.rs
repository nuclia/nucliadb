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

use nucliadb_core::prelude::*;
use nucliadb_core::protos::shard_created::{
    DocumentService, ParagraphService, RelationService, VectorService,
};
use nucliadb_core::protos::{OpStatus, Resource, ResourceId, VectorSetId, VectorSimilarity};
use nucliadb_core::tracing::{self, *};
use nucliadb_core::{thread, IndexFiles};
use nucliadb_procs::measure;
use tokio::sync::Mutex;

use crate::disk_structure::*;
use crate::shards::metadata::{ShardMetadata, Similarity};
use crate::shards::versions::Versions;
use crate::telemetry::run_with_telemetry;

#[derive(Debug)]
pub struct ShardWriter {
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
    pub gc_lock: Mutex<()>, // lock to be able to do GC or not
    write_lock: Mutex<()>,  // be able to lock writes on the shard
}

impl ShardWriter {
    #[tracing::instrument(skip_all)]
    fn initialize(
        id: String,
        path: &Path,
        metadata: ShardMetadata,
        tsc: TextConfig,
        psc: ParagraphConfig,
        vsc: VectorConfig,
        rsc: RelationConfig,
    ) -> NodeResult<ShardWriter> {
        let versions = Versions::load_or_create(
            &path.join(VERSION_FILE),
            metadata.channel.unwrap_or_default(),
        )?;
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
            gc_lock: Mutex::new(()),
            write_lock: Mutex::new(()),
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
            2 => RelationService::RelationV2,
            i => panic!("Unknown relation version {i}"),
        }
    }
    pub fn clean_and_create(id: String, path: &Path) -> NodeResult<ShardWriter> {
        let metadata = ShardMetadata::open(&path.join(METADATA_FILE))?;
        std::fs::remove_dir_all(path)?;
        std::fs::create_dir(path)?;
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel.unwrap_or_default();

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
        ShardWriter::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }

    #[measure(actor = "shard", metric = "new")]
    pub fn new(id: String, path: &Path, metadata: ShardMetadata) -> NodeResult<ShardWriter> {
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel.unwrap_or_default();

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
        let metadata_path = path.join(METADATA_FILE);
        metadata.serialize(&metadata_path)?;

        ShardWriter::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }

    #[measure(actor = "shard", metric = "open")]
    pub fn open(id: String, path: &Path) -> NodeResult<ShardWriter> {
        let metadata_path = path.join(METADATA_FILE);
        let metadata = ShardMetadata::open(&metadata_path)?;
        let tsc = TextConfig {
            path: path.join(TEXTS_DIR),
        };

        let psc = ParagraphConfig {
            path: path.join(PARAGRAPHS_DIR),
        };

        let channel = metadata.channel.unwrap_or_default();

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

        ShardWriter::initialize(id, path, metadata, tsc, psc, vsc, rsc)
    }

    #[measure(actor = "shard", metric = "set_resource")]
    #[tracing::instrument(skip_all)]
    pub fn set_resource(&self, resource: &Resource) -> NodeResult<()> {
        let span = tracing::Span::current();

        let text_writer_service = self.text_writer.clone();
        let field_resource = resource.clone();
        let text_task = move || {
            debug!("Field service starts set_resource");
            let mut writer = text_write(&text_writer_service);
            let result = writer.set_resource(&field_resource);
            debug!("Field service ends set_resource");
            result
        };

        let paragraph_resource = resource.clone();
        let paragraph_writer_service = self.paragraph_writer.clone();
        let paragraph_task = move || {
            debug!("Paragraph service starts set_resource");
            let mut writer = paragraph_write(&paragraph_writer_service);
            let result = writer.set_resource(&paragraph_resource);
            debug!("Paragraph service ends set_resource");
            result
        };

        let vector_writer_service = self.vector_writer.clone();
        let vector_resource = resource.clone();
        let vector_task = move || {
            debug!("Vector service starts set_resource");
            let mut writer = vector_write(&vector_writer_service);
            let result = writer.set_resource(&vector_resource);
            debug!("Vector service ends set_resource");
            result
        };

        let relation_writer_service = self.relation_writer.clone();
        let relation_resource = resource.clone();
        let relation_task = move || {
            debug!("Relation service starts set_resource");
            let mut writer = relation_write(&relation_writer_service);
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

        let _lock = self.write_lock.blocking_lock();
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
        self.new_generation_id(); // VERY NAIVE, SHOULD BE DONE AFTER MERGE AS WELL
        Ok(())
    }

    #[measure(actor = "shard", metric = "remove_resource")]
    #[tracing::instrument(skip_all)]
    pub fn remove_resource(&self, resource: &ResourceId) -> NodeResult<()> {
        let span = tracing::Span::current();

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

        let _lock = self.write_lock.blocking_lock();
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

        self.new_generation_id();

        Ok(())
    }

    #[measure(actor = "shard", metric = "get_opstatus")]
    #[tracing::instrument(skip_all)]
    pub fn get_opstatus(&self) -> NodeResult<OpStatus> {
        let span = tracing::Span::current();

        let paragraphs = self.paragraph_writer.clone();
        let vectors = self.vector_writer.clone();
        let texts = self.text_writer.clone();

        let count_fields = || {
            run_with_telemetry(info_span!(parent: &span, "field count"), move || {
                text_read(&texts).count()
            })
        };
        let count_paragraphs = || {
            run_with_telemetry(info_span!(parent: &span, "paragraph count"), move || {
                paragraph_read(&paragraphs).count()
            })
        };
        let count_vectors = || {
            run_with_telemetry(info_span!(parent: &span, "vector count"), move || {
                vector_read(&vectors).count()
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

        self.new_generation_id();

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub fn remove_vectorset(&self, setid: &VectorSetId) -> NodeResult<()> {
        let mut writer = vector_write(&self.vector_writer);
        writer.remove_vectorset(setid)?;

        self.new_generation_id();

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
        let _lock = self.gc_lock.blocking_lock();
        vector_write(&self.vector_writer).garbage_collection()
    }

    #[tracing::instrument(skip_all)]
    pub fn get_generation_id(&self) -> String {
        // we can not cache this on the ShardWriter instance unless we get swap our
        // Arc implementation for a RwLock implementation because we'd need
        // to write the cache on modification

        let filepath = self.path.join(GENERATION_FILE);
        // check if file does not exist
        if !filepath.exists() {
            return self.new_generation_id();
        }
        std::fs::read_to_string(filepath).unwrap()
    }

    #[tracing::instrument(skip_all)]
    pub fn new_generation_id(&self) -> String {
        let generation_id = uuid::Uuid::new_v4().to_string();
        self.set_generation_id(generation_id.clone());
        generation_id
    }

    pub fn set_generation_id(&self, generation_id: String) {
        let filepath = self.path.join(GENERATION_FILE);
        std::fs::write(filepath, generation_id).unwrap();
    }

    pub fn get_kbid(&self) -> String {
        self.metadata.kbid().unwrap_or_default().to_string()
    }

    pub fn get_similarity(&self) -> Similarity {
        self.metadata.similarity().into()
    }

    pub fn get_shard_segments(&self) -> NodeResult<HashMap<String, Vec<String>>> {
        let mut segments = HashMap::new();

        segments.insert(
            "paragraph".to_string(),
            paragraph_read(&self.paragraph_writer).get_segment_ids()?,
        );
        segments.insert(
            "text".to_string(),
            text_read(&self.text_writer).get_segment_ids()?,
        );
        segments.insert(
            "vector".to_string(),
            vector_read(&self.vector_writer).get_segment_ids()?,
        );

        Ok(segments)
    }

    pub fn get_shard_files(
        &self,
        ignored_segement_ids: &HashMap<String, Vec<String>>,
    ) -> NodeResult<Vec<IndexFiles>> {
        let mut files = Vec::new();
        let _lock = self.write_lock.blocking_lock(); // need to make sure more writes don't happen while we are reading

        files.push(
            paragraph_read(&self.paragraph_writer)
                .get_index_files(ignored_segement_ids.get("paragraph").unwrap_or(&Vec::new()))?,
        );
        files.push(
            text_read(&self.text_writer)
                .get_index_files(ignored_segement_ids.get("text").unwrap_or(&Vec::new()))?,
        );
        files.push(
            vector_read(&self.vector_writer)
                .get_index_files(ignored_segement_ids.get("vector").unwrap_or(&Vec::new()))?,
        );

        Ok(files)
    }
}
