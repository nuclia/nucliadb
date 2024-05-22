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
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.
//

//! The following code works because of the Tantivy version used in this project. Inner Tantivy
//! files that are not meant to be used are used, this is basically a hack for implementing safe
//! replication at this moment in time. It should not be seen as the way of doing this, but as a way
//! last resort.
//! Ideally we should find a way of implementing safe replication inside Tantivy.

use std::fs::File;
use std::path::{Path, PathBuf};

use serde::Serialize;
use serde_json::Value;
use tantivy::schema::Schema;
use tantivy::{Index, IndexSettings, Opstamp, Result, SegmentId};

pub type Json = Value;

/// Using a [`Searcher`] as a guard ensures us that until the
/// value is dropped the files it has access to are not modified.

pub struct TantivyReplicaState {
    pub files: Vec<(PathBuf, File)>,
    pub metadata_path: PathBuf,
    pub index_metadata: Json,
}
impl TantivyReplicaState {
    pub fn metadata_as_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(&self.index_metadata).unwrap()
    }
}

#[derive(Serialize)]
struct SegmentDeletes {
    num_deleted_docs: u32,
    opstamp: u64,
}

#[derive(Serialize)]
struct SegmentSafeMetadata {
    segment_id: SegmentId,
    max_doc: u32,
    deletes: Option<SegmentDeletes>,
}

#[derive(Serialize)]
struct SafeMetadata {
    index_settings: IndexSettings,
    segments: Vec<SegmentSafeMetadata>,
    schema: Schema,
    opstamp: Opstamp,
    #[serde(skip_serializing_if = "Option::is_none")]
    payload: Option<String>,
}

/// Needed by [`compute_safe_replica_state`]
#[derive(Clone, Copy)]
pub struct ReplicationParameters<'a> {
    /// Where the index to be replicated is located on disk
    pub path: &'a Path,
    /// IDs of the segments already present at the replica.
    pub on_replica: &'a [String],
}

pub fn compute_safe_replica_state(params: ReplicationParameters, index: &Index) -> Result<TantivyReplicaState> {
    let searcher = index.reader()?.searcher();
    let index_metadata = index.load_metas()?;
    let mut segment_files = vec![];
    let mut add_segment_file = |rel_path: PathBuf| -> Result<()> {
        segment_files.push((rel_path.clone(), File::open(params.path.join(rel_path))?));
        Ok(())
    };
    let mut safe_metadata = SafeMetadata {
        index_settings: index_metadata.index_settings,
        schema: index_metadata.schema,
        opstamp: index_metadata.opstamp,
        payload: index_metadata.payload,
        segments: vec![],
    };
    for segment in searcher.segment_readers() {
        let segment_id = segment.segment_id();
        let raw_segment_id = segment_id.uuid_string();
        let max_doc = segment.max_doc();
        let num_deleted_docs = segment.num_deleted_docs();
        let delete_opstamp = segment.delete_opstamp();
        let deletes = delete_opstamp.map(|opstamp| SegmentDeletes {
            opstamp,
            num_deleted_docs,
        });

        safe_metadata.segments.push(SegmentSafeMetadata {
            deletes,
            max_doc,
            segment_id,
        });

        let deletes = delete_opstamp.map(|stamp| PathBuf::from(format!("{raw_segment_id}.{stamp}.del")));

        if let Some(deletes) = deletes {
            add_segment_file(deletes)?;
        }

        if params.on_replica.contains(&raw_segment_id) {
            // If this segment is found on the replica we just
            // send the deletes file. The rest is immutable so its
            // already there.
            continue;
        }

        let postings = PathBuf::from(format!("{raw_segment_id}.idx"));
        let positions = PathBuf::from(format!("{raw_segment_id}.pos"));
        let terms = PathBuf::from(format!("{raw_segment_id}.term"));
        let store = PathBuf::from(format!("{raw_segment_id}.store"));
        let fast_fields = PathBuf::from(format!("{raw_segment_id}.fast"));
        let field_norms = PathBuf::from(format!("{raw_segment_id}.fieldnorm"));

        if params.path.join(&postings).exists() {
            add_segment_file(postings)?;
        }
        if params.path.join(&positions).exists() {
            add_segment_file(positions)?;
        }
        if params.path.join(&terms).exists() {
            add_segment_file(terms)?;
        }
        if params.path.join(&store).exists() {
            add_segment_file(store)?;
        }
        if params.path.join(&fast_fields).exists() {
            add_segment_file(fast_fields)?;
        }
        if params.path.join(&field_norms).exists() {
            add_segment_file(field_norms)?;
        }
    }

    Ok(TantivyReplicaState {
        files: segment_files,
        metadata_path: PathBuf::from("meta.json"),
        index_metadata: serde_json::to_value(safe_metadata)?,
    })
}

#[cfg(test)]
mod tests {
    use std::fs::File;

    use tantivy::doc;
    use tantivy::schema::{Field, Schema, STORED, TEXT};

    use super::*;

    struct TestingSchema {
        text_field: Field,
        schema: Schema,
    }

    fn define_schema() -> TestingSchema {
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT | STORED);
        let schema = schema_builder.build();
        TestingSchema {
            text_field,
            schema,
        }
    }

    #[test]
    fn metadata_file_validation() {
        let workspace = tempfile::tempdir().unwrap();
        let testing_schema = define_schema();
        let index = Index::create_in_dir(workspace.path(), testing_schema.schema).unwrap();
        let mut writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
        writer.add_document(doc!(testing_schema.text_field => "HI IM TEXT")).unwrap();
        writer.commit().unwrap();

        let replica_params = ReplicationParameters {
            path: workspace.path(),
            on_replica: &[],
        };
        let metadata_as_json = serde_json::to_value(index.load_metas().unwrap()).unwrap();
        let safe_replica_state = compute_safe_replica_state(replica_params, &index).unwrap();
        assert_eq!(safe_replica_state.index_metadata, metadata_as_json);
    }

    #[test]
    fn recreate_using_safe_state() {
        let workspace = tempfile::tempdir().unwrap();
        let testing_schema = define_schema();
        let index = Index::create_in_dir(&workspace, testing_schema.schema).unwrap();
        let mut writer = index.writer_with_num_threads(1, 6_000_000).unwrap();
        writer.add_document(doc!(testing_schema.text_field => "ONE")).unwrap();
        writer.commit().unwrap();

        let replica_params = ReplicationParameters {
            path: workspace.path(),
            on_replica: &[],
        };
        let safe_state = compute_safe_replica_state(replica_params, &index).unwrap();

        let replica_workspace = tempfile::tempdir().unwrap();
        let mut metadata_file = File::create(replica_workspace.path().join("meta.json")).unwrap();
        serde_json::to_writer(&mut metadata_file, &safe_state.index_metadata).unwrap();

        for (path_in_replica, ref mut file_in_replica) in safe_state.files {
            let mut target = File::create(replica_workspace.path().join(path_in_replica)).unwrap();
            std::io::copy(file_in_replica, &mut target).unwrap();
        }

        let replica_index = Index::open_in_dir(replica_workspace.path()).unwrap();
        let reader = replica_index.reader().unwrap();
        let searcher = reader.searcher();
        let collector = tantivy::collector::DocSetCollector;
        let docs = searcher.search(&tantivy::query::AllQuery, &collector).unwrap();

        assert_eq!(docs.len(), 1);
    }
}
