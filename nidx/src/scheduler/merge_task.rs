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

use std::collections::HashMap;

use crate::{
    NidxMetadata,
    metadata::{IndexId, IndexKind, MergeJob, SegmentId},
    settings::MergeSettings,
};
use nidx_types::Seq;

use super::{log_merge, vector_merge};

pub struct MergeScheduler {
    settings: MergeSettings,
}

impl MergeScheduler {
    pub fn from_settings(settings: MergeSettings) -> Self {
        Self { settings }
    }

    /// Enqueue merge jobs for segments older than `last_indexed_seq` that aren't
    /// already scheduled for merge or marked to delete.
    ///
    /// Merging involves creation of a single segment from multiple ones, combining
    /// their data and applying deletions. Merge jobs are executed in parallel (in
    /// multiple workers) and while other segments are being indexed. This restricts
    /// us to only merge segments whose sequences are less than the smaller sequence
    /// being indexed.
    ///
    /// As an example, if sequences 100 and 102 are indexed but 101 is still being
    /// indexed, we can only merge segments with sequence <= 100. Otherwise, if we
    /// merge 100 and 102 (generating a new 102 segment) and segment 101 included
    /// deletions for 100, we'll never apply them and we'll end in an inconsistent
    /// state.
    pub async fn schedule_merges(&self, meta: &NidxMetadata, last_indexed_seq: Seq) -> anyhow::Result<()> {
        // TODO: Remove vector index split by metadata when removing tags
        let indexes = sqlx::query!(
            r#"
                -- For each index, gather the seq number before which we want to force deletion of segments
                -- e.g: gather the seq of the 100th newest deletion, and force merge all segments older than that
                WITH index_deletion_window AS (
                    SELECT
                        id,
                        COALESCE(
                            (
                                -- Biggest seq excluding the N most recent
                                SELECT MIN(seq) FROM (
                                    SELECT seq FROM deletions WHERE index_id = indexes.id ORDER BY seq DESC LIMIT $2)
                                    AS new_deletions
                                ),
                            0
                        ) AS seq
                    FROM indexes
                    JOIN deletions ON index_id = indexes.id
                    GROUP BY indexes.id
                    HAVING COUNT(*) > $2
                )
                -- For each index, a list of mergeable segments with amount of records and deletions
                SELECT
                    indexes.id AS "id: IndexId",
                    indexes.kind AS "kind: IndexKind",
                    array_agg(
                        -- segment_id, records, force_merge (has more than N deletions)
                        (segments.id, records, segments.seq < COALESCE(index_deletion_window.seq, 0))
                        ORDER BY records DESC
                    ) AS "segments!: Vec<(SegmentId, i64, bool)>"
                FROM segments
                JOIN indexes ON segments.index_id = indexes.id
                LEFT JOIN index_deletion_window ON segments.index_id = index_deletion_window.id
                WHERE
                    delete_at IS NULL AND merge_job_id IS NULL
                    AND segments.seq <= $1
                GROUP BY
                    indexes.id,
                    indexes.kind,
                    -- Only merge vector segments with same tags
                    CASE WHEN kind = 'vector' THEN index_metadata::text ELSE NULL END,
                    index_deletion_window.id
                -- Only return indexes with more than one segment or with many deletions
                HAVING COUNT(segments) > 1 OR index_deletion_window.id IS NOT NULL
            "#,
            i64::from(last_indexed_seq),
            self.settings.max_deletions as i64
        )
        .fetch_all(&meta.pool)
        .await?;

        for index in indexes {
            let segment_info: HashMap<_, _> = index.segments.iter().map(|(id, rec, f)| (*id, (*rec, *f))).collect();
            let merges = match index.kind {
                IndexKind::Text | IndexKind::Paragraph | IndexKind::Relation => {
                    log_merge::plan_merges(&self.settings.log_merge, index.segments)
                }
                IndexKind::Vector => vector_merge::plan_merges(&self.settings.vector_merge, index.segments),
            };

            for m in merges {
                let (records, forced) = m.iter().fold((0, false), |(records, forced), sid| {
                    let segment = segment_info.get(sid).unwrap();
                    (records + segment.0, forced || segment.1)
                });
                let priority = calculate_priority(m.len(), records, forced);
                MergeJob::create(meta, index.id, &m, last_indexed_seq, priority).await?;
            }
        }

        Ok(())
    }
}

/// Calculate a priority for the job. We want higher priority when:
/// - Merging many segments
/// - Merging small segments (will take short time)
/// - Merging because of deletions
fn calculate_priority(segments: usize, records: i64, forced: bool) -> i32 {
    let forced_score = if forced { 5 } else { 0 };
    segments as i32 + forced_score - records as i32 / 10_000
}

#[cfg(test)]
mod tests {
    use super::*;

    mod merge_scheduling {
        use std::collections::{HashMap, HashSet};

        use nidx_types::Seq;
        use nidx_vector::config::{VectorCardinality, VectorConfig};
        use serde_json::json;
        use uuid::Uuid;

        use super::*;

        use crate::{
            metadata::{Deletion, Index, IndexConfig, IndexId, NidxMetadata, Segment, Shard},
            settings::LogMergeSettings,
        };

        const VECTOR_CONFIG: VectorConfig = VectorConfig {
            similarity: nidx_vector::config::Similarity::Cosine,
            normalize_vectors: false,
            vector_type: nidx_vector::config::VectorType::DenseF32 { dimension: 3 },
            flags: vec![],
            vector_cardinality: VectorCardinality::Single,
        };

        fn merge_scheduler() -> MergeScheduler {
            MergeScheduler::from_settings(MergeSettings {
                log_merge: LogMergeSettings {
                    min_number_of_segments: 3,
                    ..Default::default()
                },
                ..Default::default()
            })
        }

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_single_index(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;
            let mut seq: i64 = 0;

            for _ in 0..10 {
                let segment =
                    Segment::create(&meta.pool, index.id, Seq::from(seq), 50, serde_json::Value::Null).await?;
                segment.mark_ready(&meta.pool, 1000).await?;
                seq += 1;
            }

            let last_seq = Seq::from(seq - 1);

            // creation of shards/indexes/segments don't trigger any merge job
            assert!(MergeJob::take(&meta.pool).await?.is_none());

            merge_scheduler().schedule_merges(&meta, last_seq).await?;

            // one job has been scheduled for the index
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].index_id, index.id);
            assert_eq!(jobs[0].seq, last_seq);

            for segment in index.segments(&meta.pool).await? {
                assert!(segment.merge_job_id.is_some());
                assert_eq!(segment.merge_job_id.unwrap(), jobs[0].id);
            }

            Ok(())
        }

        #[sqlx::test]
        async fn test_schedule_merges_for_shard_with_multiple_indexes(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;

            let indexes = vec![
                Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?,
                Index::create(&meta.pool, shard.id, "english", VECTOR_CONFIG.into()).await?,
                Index::create(&meta.pool, shard.id, "fulltext", IndexConfig::new_text()).await?,
                Index::create(&meta.pool, shard.id, "keyword", IndexConfig::new_paragraph()).await?,
                Index::create(&meta.pool, shard.id, "relation", IndexConfig::new_relation()).await?,
            ];
            let mut seq: i64 = 0;

            for _ in 0..10 {
                for index in &indexes {
                    let segment =
                        Segment::create(&meta.pool, index.id, Seq::from(seq), 50, serde_json::Value::Null).await?;
                    segment.mark_ready(&meta.pool, 1000).await?;
                    seq += 1;
                }
            }

            merge_scheduler().schedule_merges(&meta, Seq::from(seq)).await?;

            // scheduled a job per index
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), indexes.len());
            assert_eq!(
                indexes.iter().map(|i| i.id).collect::<HashSet<_>>(),
                jobs.iter().map(|j| j.index_id).collect::<HashSet<_>>(),
            );

            for job in &jobs {
                assert_eq!(job.seq, Seq::from(seq));
            }

            // validate segments are marked with merge job id
            let jobs_by_index: HashMap<IndexId, _> = jobs.iter().map(|job| (job.index_id, job)).collect();
            for index in &indexes {
                let (_, merge_job) = jobs_by_index.get_key_value(&index.id).unwrap();

                let segments = index.segments(&meta.pool).await?;
                assert_eq!(segments.len(), 10);

                for segment in &segments {
                    assert!(segment.merge_job_id.is_some());
                    assert_eq!(segment.merge_job_id.unwrap(), merge_job.id);
                }
            }

            Ok(())
        }

        async fn ongoing_indexing_scenario(pool: sqlx::PgPool) -> anyhow::Result<NidxMetadata> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

            for seq in [95, 98, 99, 100, 102i64] {
                let segment =
                    Segment::create(&meta.pool, index.id, Seq::from(seq), 50, serde_json::Value::Null).await?;
                segment.mark_ready(&meta.pool, 1000).await?;
            }

            // 101 is still indexing
            Segment::create(&meta.pool, index.id, Seq::from(101i64), 50, serde_json::Value::Null).await?;

            Ok(meta)
        }

        #[sqlx::test]
        async fn scheduling_with_smaller_than_existing_sequences(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = ongoing_indexing_scenario(pool).await?;

            merge_scheduler().schedule_merges(&meta, Seq::from(50i64)).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert!(jobs.is_empty());

            Ok(())
        }

        #[sqlx::test]
        async fn scheduling_with_ack_floor(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = ongoing_indexing_scenario(pool).await?;
            let ack_floor = Seq::from(100i64);

            merge_scheduler().schedule_merges(&meta, ack_floor).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 1);

            let index = Index::get(&meta.pool, jobs[0].index_id).await?;
            let segments = index.segments(&meta.pool).await?;

            let mut segment_sequences = HashSet::new();
            for segment in &segments {
                if segment.seq <= ack_floor {
                    // should be scheduled to merge
                    assert!(segment.merge_job_id.is_some());
                    assert_eq!(segment.merge_job_id.unwrap(), jobs[0].id);
                    segment_sequences.insert(segment.seq);
                } else {
                    assert!(segment.merge_job_id.is_none());
                }
            }
            let expected = [95, 98, 99, 100i64].into_iter().map(Seq::from).collect();
            assert_eq!(segment_sequences, expected);

            Ok(())
        }

        #[sqlx::test]
        async fn test_schedule_merges_with_segment_tags(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let merge_scheduler = MergeScheduler::from_settings(MergeSettings {
                log_merge: LogMergeSettings {
                    min_number_of_segments: 2,
                    ..Default::default()
                },
                ..Default::default()
            });
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;

            let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

            let hidden_count = 1;
            let visible_count = 2;

            let segment_hidden = Segment::create(
                &meta.pool,
                index.id,
                1i64.into(),
                hidden_count,
                json!({"tags": ["/q/h"]}),
            )
            .await?;
            segment_hidden.mark_ready(&meta.pool, 1000).await?;

            let segment_visible =
                Segment::create(&meta.pool, index.id, 2i64.into(), visible_count, json!({"tags": []})).await?;
            segment_visible.mark_ready(&meta.pool, 1000).await?;

            // Cannot merge (two segments with different tags)
            merge_scheduler.schedule_merges(&meta, 6i64.into()).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert!(jobs.is_empty());

            let segment_hidden2 = Segment::create(
                &meta.pool,
                index.id,
                3i64.into(),
                hidden_count,
                json!({"tags": ["/q/h"]}),
            )
            .await?;
            segment_hidden2.mark_ready(&meta.pool, 1000).await?;

            let segment_visible2 =
                Segment::create(&meta.pool, index.id, 4i64.into(), visible_count, json!({"tags": []})).await?;
            segment_visible2.mark_ready(&meta.pool, 1000).await?;

            // Can merge hidden and visible segments pair-wise
            merge_scheduler.schedule_merges(&meta, 6i64.into()).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 2);

            for j in jobs {
                let merged_segments = j.segments(&meta.pool).await?;
                assert_eq!(merged_segments.len(), 2);
                // We use different number of records to differentiante hidden/visible segments
                // because the tags field is private outside the vectors index
                assert_eq!(merged_segments[0].records, merged_segments[1].records);
            }

            Ok(())
        }

        async fn get_all_merge_jobs(meta: &NidxMetadata) -> anyhow::Result<Vec<MergeJob>> {
            let mut jobs = vec![];
            while let Some(job) = MergeJob::take(&meta.pool).await? {
                jobs.push(job);
            }
            Ok(jobs)
        }

        #[sqlx::test]
        async fn test_schedule_merges_with_many_deletions(pool: sqlx::PgPool) -> anyhow::Result<()> {
            let meta = NidxMetadata::new_with_pool(pool).await?;
            let kbid = Uuid::new_v4();
            let shard = Shard::create(&meta.pool, kbid).await?;
            let index = Index::create(&meta.pool, shard.id, "multilingual", VECTOR_CONFIG.into()).await?;

            let segment = Segment::create(&meta.pool, index.id, 1i64.into(), 50, serde_json::Value::Null).await?;
            segment.mark_ready(&meta.pool, 1000).await?;

            let merge_scheduler = MergeScheduler::from_settings(MergeSettings {
                max_deletions: 2,
                ..Default::default()
            });

            // Just one segment, no need to merge
            merge_scheduler.schedule_merges(&meta, 5i64.into()).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 0);

            // Add some deletions
            for s in 0i64..5i64 {
                Deletion::create(&meta.pool, index.id, s.into(), &[]).await?;
            }

            // one job has been scheduled for the index
            merge_scheduler.schedule_merges(&meta, 5i64.into()).await?;
            let jobs = get_all_merge_jobs(&meta).await?;
            assert_eq!(jobs.len(), 1);
            assert_eq!(jobs[0].index_id, index.id);
            assert_eq!(jobs[0].seq, 5i64.into());

            Ok(())
        }
    }
}
