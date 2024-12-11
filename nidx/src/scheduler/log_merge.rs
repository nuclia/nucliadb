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

use crate::{metadata::SegmentId, settings::MergeSettings};
use tracing::*;

/// [`LogLogMergeStrategy`] implements a logarithmic merge strategy inspired from
/// tantivy's log merge policy. The algorithm tries to merge segments with
/// similar number of records.
///
/// It works by splitting up segments in different buckets depending on the
/// amount of records they contain. Segments within a bucket are then scheduled
/// to be merged together (if there are more than `min_number_of_segments`).
///
/// Buckets are dynamically computed depending on the segments. The biggest one
/// marks the top bucket, and a configured bucket size marks the next ones.
///
/// To avoid merging segments too large or dividing in too many buckets for
/// small segments, one can configure the top and bottom bucket ceilings.
///
/// Here's a graphical representation:
///
/// ```text
///                              |   (too big to   |
///                              |    be merged)   |
///  `top_bucket_max_records` -> +-----------------+
///                              |  (no segments)  |
///                              +-----------------+ <- log2(biggest_segment.records)
///                              |   top bucket    |
///                              +-----------------+ <- log2(biggest_segment.records) - bucket_size_log
///                              |                 |
///                              +-----------------+ <- log2(biggest_segment.records) - 2 * bucket_size_log
///                              |                 |
///                              |       ...       |
///                              |                 |
///                          +-- +-----------------+
///       `bucket_size_log` -+   |                 |
///                          +-- +-----------------+
///                              |                 |
///                              +-----------------+ <- log2(biggest_segment.records) - n * bucket_size_log
/// `bottom_bucket_threshold` -> +  -   -   -  -  -|
///                              |  bottom bucket  |
///                              |                 |
///                              |   (too small    |
///                              |    to split)    |
/// ```
///
pub(super) struct LogMergeStrategy {
    /// Minimum number of segments needed to perform a merge for an index
    min_number_of_segments: usize,

    /// Max number of records for a segment to be in the top bucket, i.e.,
    /// elegible for merge. Once a segment becomes bigger, it won't be merged
    /// anymore
    top_bucket_max_records: usize,

    /// Max number of records for a segment to be considered in the bottom
    /// bucket. Segments with fewer records won't be further splitted in buckets
    bottom_bucket_threshold: usize,

    /// Log value between buckets. Increasing this number implies more segment
    /// sizes to be grouped in the same merge job.
    bucket_size_log: f64,
}

impl Default for LogMergeStrategy {
    fn default() -> Self {
        Self {
            min_number_of_segments: 4,
            top_bucket_max_records: 10_000_000,
            bottom_bucket_threshold: 10_000,
            bucket_size_log: 0.75,
        }
    }
}

impl LogMergeStrategy {
    pub fn from_settings(settings: &MergeSettings) -> Self {
        Self {
            min_number_of_segments: settings.min_number_of_segments as usize,
            top_bucket_max_records: settings.max_segment_size,
            ..Default::default()
        }
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
    pub fn plan_merges(&self, segments: Vec<(SegmentId, i64)>) -> Vec<Vec<SegmentId>> {
        let mut buckets = vec![];
        let mut current_bucket = vec![];
        let mut current_max_size_log = f64::MAX;

        for (segment_id, records) in segments {
            let segment_size_log = f64::from(std::cmp::max(records as u32, self.bottom_bucket_threshold as u32)).log2();
            if segment_size_log <= (current_max_size_log - self.bucket_size_log) {
                // traversed to next bucket, store current and continue
                buckets.push(current_bucket);
                current_bucket = vec![];
                current_max_size_log = segment_size_log;
            }

            current_bucket.push((segment_id, records));
        }
        buckets.push(current_bucket);

        let mut merges = Vec::new();
        for segments in buckets {
            // TODO: merge segments with too many deletions
            if segments.len() >= self.min_number_of_segments {
                let mut sum_segments = 0;
                let mut to_merge = Vec::new();
                for (sid, records) in &segments {
                    sum_segments += *records as usize;
                    to_merge.push(*sid);
                    if sum_segments > self.top_bucket_max_records {
                        if to_merge.len() >= self.min_number_of_segments {
                            debug!(?to_merge, "Scheduling merge job for bucket");
                            merges.push(std::mem::take(&mut to_merge));
                        }
                        sum_segments = 0;
                        to_merge.clear();
                    }
                }

                if to_merge.len() >= self.min_number_of_segments {
                    debug!(?to_merge, "Scheduling merge job for bucket");
                    merges.push(to_merge);
                }
            }
        }

        merges
    }
}

#[cfg(test)]
mod tests {
    use crate::{scheduler::log_merge::LogMergeStrategy, settings::MergeSettings};

    #[test]
    fn test_log_merge_scheduling_not_enough_segments_merge() -> anyhow::Result<()> {
        let log_merge = LogMergeStrategy::from_settings(&MergeSettings {
            min_number_of_segments: 4,
            ..Default::default()
        });
        let jobs = log_merge.plan_merges(vec![(1i64.into(), 50), (2i64.into(), 50), (3i64.into(), 50)]);
        assert!(jobs.is_empty());

        Ok(())
    }

    #[test]
    fn test_log_merge_scheduling_same_size_segments() -> anyhow::Result<()> {
        let log_merge = LogMergeStrategy::from_settings(&MergeSettings {
            min_number_of_segments: 3,
            ..Default::default()
        });
        let jobs = log_merge.plan_merges(vec![(1i64.into(), 50), (2i64.into(), 50), (3i64.into(), 50)]);

        // all segments have been scheduled for merge in a single job
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].len(), 3);

        Ok(())
    }

    #[test]
    fn test_log_merge_scheduling_all_buckets() -> anyhow::Result<()> {
        let log_merge = LogMergeStrategy {
            min_number_of_segments: 2,
            top_bucket_max_records: 1000,
            bottom_bucket_threshold: 50,
            bucket_size_log: 1.0,
        };

        let jobs = log_merge.plan_merges(vec![
            (7i64.into(), 1001), // exceeds the max segment size, won't appear
            (3i64.into(), 1000), // log2(1000) = ~9.97 -- will mark the top bucket
            (12i64.into(), 501), // last element in top bucket
            (13i64.into(), 500), // just below top bucket
            (11i64.into(), 249), // top - 2
            (9i64.into(), 125),  // top - 2
            (5i64.into(), 124),  // bottom + 1
            (4i64.into(), 63),   // bottom + 1
            (6i64.into(), 62),   // first in bottom bucket
            (10i64.into(), 51),  // just above bottom_bucket_threshold
            (1i64.into(), 50),   // bottom bucket
            (2i64.into(), 10),   // bottom bucket
            (8i64.into(), 20),   // bottom bucket
        ]);

        println!("{jobs:?}");
        assert_eq!(jobs.len(), 4);

        // For test simplicity, we rely on jobs being created in
        // top-to-bottom order. Feel free to change this is the algorithm
        // changes

        // Top bucket (501, 1000)
        assert_eq!(jobs[0], vec![3i64.into(), 12i64.into()]);
        // 125, 249
        assert_eq!(jobs[1], vec![11i64.into(), 9i64.into()]);
        // 63, 124
        assert_eq!(jobs[2], vec![5.into(), 4i64.into()]);
        // Bottom bucket (10, 20, 50, 51, 62)
        assert_eq!(jobs[3], vec![6i64.into(), 10i64.into(), 1i64.into(), 2i64.into(), 8i64.into()]);

        // Not merged
        // 1001 -> top_bucket_max_records
        // 500  -> min_number_of_segments

        Ok(())
    }
}
