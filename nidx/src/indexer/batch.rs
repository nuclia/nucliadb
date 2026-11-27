// Copyright 2021 Bosutech XXI S.L.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use nidx_protos::prost::*;
use nidx_protos::{IndexResourcesRequest, Resource, resource::ResourceStatus};
use nidx_types::{SegmentMetadata, Seq};
use object_store::DynObjectStore;
use tokio::task::JoinSet;
use tonic::Status;
use tracing::*;
use uuid::Uuid;

use crate::metadata::{Deletion, Index, IndexId, IndexKind, NewSegment, NidxMetadata, Segment};
use crate::metrics::IndexKindLabels;
use crate::metrics::indexer::{PER_INDEX_INDEXING_TIME, TOTAL_INDEXING_TIME};
use crate::segment_store::pack_and_upload;

pub(crate) const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024;
pub(crate) const MAX_RESOURCES: usize = 1000;

pub(super) fn validate_request(request: &IndexResourcesRequest) -> Result<Uuid, Status> {
    let shard_id = Uuid::parse_str(&request.shard_id).map_err(|_| Status::invalid_argument("invalid shard ID"))?;
    if request.resources.is_empty() || request.resources.len() > MAX_RESOURCES {
        return Err(Status::invalid_argument(
            "resources must contain between 1 and 1000 items",
        ));
    }
    if request.encoded_len() > MAX_BATCH_BYTES {
        return Err(Status::resource_exhausted("indexed batch exceeds gRPC message limit"));
    }
    let mut ids = HashSet::with_capacity(request.resources.len());
    for resource in &request.resources {
        let id = resource
            .resource
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("resource ID required"))?;
        let uuid = Uuid::parse_str(&id.uuid).map_err(|_| Status::invalid_argument("invalid resource ID"))?;
        if id.shard_id != request.shard_id || resource.shard_id != request.shard_id {
            return Err(Status::invalid_argument("shard IDs must match"));
        }
        if !ids.insert(uuid) {
            return Err(Status::invalid_argument("duplicate resource ID in indexed batch"));
        }
        if resource.status == ResourceStatus::Delete as i32 {
            return Err(Status::invalid_argument(
                "batch resources must be upserts, not deletions",
            ));
        }
    }
    Ok(shard_id)
}

type BatchIndexingResult = (IndexId, Vec<(Segment, usize)>, Vec<String>);
type BuiltSegments = Vec<(PathBuf, NewSegment)>;

#[instrument(skip_all, fields(%shard_id, records = resources.len()))]
pub(super) async fn index_resources(
    meta: &NidxMetadata,
    storage: Arc<DynObjectStore>,
    work_path: &Path,
    shard_id: Uuid,
    resources: Vec<Resource>,
    seq: Seq,
) -> anyhow::Result<()> {
    let start = Instant::now();
    let indexes = Index::for_shard(&meta.pool, shard_id).await?;
    let single_vector_index = indexes.iter().filter(|index| index.kind == IndexKind::Vector).count() == 1;
    let resources = Arc::new(resources);
    let mut tasks: JoinSet<anyhow::Result<BatchIndexingResult>> = JoinSet::new();
    for index in indexes {
        let resources = Arc::clone(&resources);
        let meta = meta.clone();
        let storage = Arc::clone(&storage);
        let output_dir = tempfile::tempdir_in(work_path)?;
        tasks.spawn(
            async move {
                let path = output_dir.path().to_path_buf();
                let index_copy = index.clone();
                let span = Span::current();
                let (built, deletions) = tokio::task::spawn_blocking(move || {
                    span.in_scope(|| build_segments(&index_copy, &resources, &path, single_vector_index))
                })
                .await??;
                let mut segments = Vec::with_capacity(built.len());
                for (path, built) in built {
                    let segment =
                        Segment::create(&meta.pool, index.id, seq, built.records, built.index_metadata).await?;
                    let size = pack_and_upload(Arc::clone(&storage), &path, segment.id.storage_key()).await?;
                    segments.push((segment, size));
                }
                Ok((index.id, segments, deletions))
            }
            .in_current_span(),
        );
    }
    let results: anyhow::Result<Vec<_>> = tasks.join_all().await.into_iter().collect();
    let results = results?;
    let mut tx = meta.transaction().await?;
    let mut updated = Vec::new();
    for (index_id, segments, deletions) in results {
        if segments.is_empty() && deletions.is_empty() {
            continue;
        }
        for (segment, size) in segments {
            segment.mark_ready(&mut *tx, size as i64).await?;
        }
        if !deletions.is_empty() {
            Deletion::create(&mut *tx, index_id, seq, &deletions).await?;
        }
        updated.push(index_id);
    }
    tx.commit().await?;
    Index::updated_many(&meta.pool, &updated).await?;
    TOTAL_INDEXING_TIME.observe(start.elapsed().as_secs_f64());
    Ok(())
}

fn built_segment<M: serde::Serialize>(segment: SegmentMetadata<M>) -> (PathBuf, NewSegment) {
    (segment.path.clone(), segment.into())
}

fn build_segments(
    index: &Index,
    resources: &[Resource],
    path: &Path,
    single_vector_index: bool,
) -> anyhow::Result<(BuiltSegments, Vec<String>)> {
    let start = Instant::now();
    let segments = match index.kind {
        IndexKind::Vector | IndexKind::VectorRelationNode | IndexKind::VectorRelationEdge => nidx_vector::VectorIndexer
            .index_resources(path, &index.config()?, resources, &index.name, single_vector_index)?
            .into_iter()
            .map(built_segment)
            .collect(),
        IndexKind::Text => nidx_text::TextIndexer
            .index_resources(path, index.config()?, resources)?
            .into_iter()
            .map(built_segment)
            .collect(),
        IndexKind::Paragraph => nidx_paragraph::ParagraphIndexer
            .index_resources(path, resources)?
            .into_iter()
            .map(built_segment)
            .collect(),
        IndexKind::Relation => nidx_relation::RelationIndexer
            .index_resources(path, &index.config()?, resources)?
            .into_iter()
            .map(built_segment)
            .collect(),
        IndexKind::Json => nidx_json::JsonIndexer
            .index_resources(path, resources)?
            .into_iter()
            .map(built_segment)
            .collect(),
    };
    let relation_config = if index.kind == IndexKind::Relation {
        Some(index.config()?)
    } else {
        None
    };
    let mut deletions = HashSet::new();
    for resource in resources {
        deletions.extend(match index.kind {
            IndexKind::Vector | IndexKind::VectorRelationNode | IndexKind::VectorRelationEdge => {
                nidx_vector::VectorIndexer.deletions_for_resource(resource, &index.name)
            }
            IndexKind::Text => nidx_text::TextIndexer.deletions_for_resource(resource),
            IndexKind::Paragraph => nidx_paragraph::ParagraphIndexer.deletions_for_resource(resource),
            IndexKind::Relation => nidx_relation::RelationIndexer.deletions_for_resource(
                relation_config.as_ref().expect("relation configuration loaded"),
                resource,
            ),
            IndexKind::Json => nidx_json::JsonIndexer.deletions_for_resource(resource),
        });
    }
    PER_INDEX_INDEXING_TIME
        .get_or_create(&IndexKindLabels::new(index.kind))
        .observe(start.elapsed().as_secs_f64());
    Ok((segments, deletions.into_iter().collect()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nidx_protos::{JsonFieldValue, ResourceId};
    use nidx_tests::little_prince;
    use nidx_vector::config::{VectorConfig, VectorType};
    use object_store::memory::InMemory;

    fn request() -> IndexResourcesRequest {
        let shard_id = Uuid::new_v4().to_string();
        IndexResourcesRequest {
            resources: vec![little_prince(&shard_id, None), little_prince(&shard_id, None)],
            shard_id,
        }
    }

    #[test]
    fn rejects_invalid_batches_before_indexing() {
        let valid = request();
        assert!(validate_request(&valid).is_ok());
        let mut invalid = valid.clone();
        invalid.resources.clear();
        assert_eq!(
            validate_request(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        invalid.resources = vec![valid.resources[0].clone(); 1001];
        assert_eq!(
            validate_request(&invalid).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut duplicate = valid.clone();
        duplicate.resources[1].resource = Some(ResourceId {
            uuid: Uuid::parse_str(&valid.resources[0].resource.as_ref().unwrap().uuid)
                .unwrap()
                .simple()
                .to_string(),
            shard_id: valid.shard_id.clone(),
        });
        assert_eq!(
            validate_request(&duplicate).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut mismatched = valid.clone();
        mismatched.resources[1].shard_id = Uuid::new_v4().to_string();
        assert_eq!(
            validate_request(&mismatched).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut missing = valid.clone();
        missing.resources[1].resource = None;
        assert_eq!(
            validate_request(&missing).unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let mut oversized = valid;
        oversized.resources[0].texts.get_mut("a/title").unwrap().text = "x".repeat(MAX_BATCH_BYTES);
        assert_eq!(
            validate_request(&oversized).unwrap_err().code(),
            tonic::Code::ResourceExhausted
        );
    }

    #[sqlx::test]
    async fn failed_index_build_does_not_publish_other_indexes(pool: sqlx::PgPool) -> anyhow::Result<()> {
        let meta = NidxMetadata::new_with_pool(pool).await?;
        let shard = crate::api::shards::create_shard(
            &meta,
            Uuid::new_v4(),
            vec![(
                "default".into(),
                VectorConfig::for_paragraphs(VectorType::DenseF32 { dimension: 3 }),
            )],
            HashSet::new(),
        )
        .await?;
        let mut resources = vec![
            little_prince(shard.id.to_string(), None),
            little_prince(shard.id.to_string(), None),
        ];
        for resource in &mut resources {
            resource.texts_to_delete = vec![resource.resource.as_ref().unwrap().uuid.clone()];
            resource
                .json_fields
                .insert("a/json".into(), JsonFieldValue { value: "{}".into() });
        }
        resources[1].json_fields.get_mut("a/json").unwrap().value = "not json".into();
        let storage = Arc::new(InMemory::new());
        let work = tempfile::tempdir()?;
        assert!(
            index_resources(&meta, storage, work.path(), shard.id, resources, 1i64.into())
                .await
                .is_err()
        );
        let ready: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM segments WHERE delete_at IS NULL")
            .fetch_one(&meta.pool)
            .await?;
        let built: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM segments")
            .fetch_one(&meta.pool)
            .await?;
        let deletions: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM deletions")
            .fetch_one(&meta.pool)
            .await?;
        assert_eq!(ready, 0);
        assert!(built > 0, "other index builds must have completed without publishing");
        assert_eq!(deletions, 0);
        Ok(())
    }
}
