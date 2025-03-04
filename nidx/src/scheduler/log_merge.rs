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

use crate::{metadata::SegmentId, settings::LogMergeSettings};

/// [`LogMergeSettings{
/// tantivy's log merge policy. The algorithm tries to merge segments with
/// similar number of records.
//
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
pub fn plan_merges(settings: &LogMergeSettings, segments: Vec<(SegmentId, i64, bool)>) -> Vec<Vec<SegmentId>> {
    let mut buckets = vec![];
    let mut current_bucket = vec![];
    let mut current_max_size_log = f64::MAX;

    let mut merges = Vec::new();
    for (segment_id, records, force) in segments {
        // Do not merge large segments except if forced to apply deletions
        if records as usize > settings.top_bucket_max_records {
            if force {
                merges.push(vec![segment_id]);
            }
            continue;
        }
        let segment_size_log = f64::from(std::cmp::max(records as u32, settings.bottom_bucket_threshold as u32)).log2();
        if segment_size_log <= (current_max_size_log - settings.bucket_size_log) {
            // traversed to next bucket, store current and continue
            buckets.push(current_bucket);
            current_bucket = vec![];
            current_max_size_log = segment_size_log;
        }

        current_bucket.push((segment_id, records, force));
    }
    buckets.push(current_bucket);

    for segments in buckets {
        // Only merge if we have enough segments or we are forced to purge deletions
        if segments.len() >= settings.min_number_of_segments || segments.iter().any(|(_, _, force)| *force) {
            let mut sum_segments = 0;
            let mut to_merge = Vec::new();

            let mut forced = false;
            for (sid, records, force) in &segments {
                forced |= force;
                sum_segments += *records as usize;
                to_merge.push(*sid);
                if sum_segments > settings.top_bucket_max_records {
                    if to_merge.len() >= settings.min_number_of_segments || forced {
                        merges.push(std::mem::take(&mut to_merge));
                    }
                    forced = false;
                    sum_segments = 0;
                    to_merge.clear();
                }
            }

            if to_merge.len() >= settings.min_number_of_segments || forced {
                merges.push(to_merge);
            }
        }
    }

    merges
}

#[cfg(test)]
mod tests {
    use crate::{scheduler::log_merge::plan_merges, settings::LogMergeSettings};

    #[test]
    fn test_log_merge_scheduling_forced_merge() -> anyhow::Result<()> {
        let log_merge = LogMergeSettings {
            min_number_of_segments: 3,
            top_bucket_max_records: 1000,
            bottom_bucket_threshold: 5,
            bucket_size_log: 1.0,
        };

        let jobs = plan_merges(&log_merge, vec![(1i64.into(), 50, false), (2i64.into(), 50, false)]);
        assert_eq!(jobs.len(), 0);

        let jobs = plan_merges(&log_merge, vec![(1i64.into(), 50, false), (2i64.into(), 50, true)]);
        assert_eq!(jobs.len(), 1);

        let jobs = plan_merges(&log_merge, vec![(1i64.into(), 2000, false)]);
        assert_eq!(jobs.len(), 0);

        let jobs = plan_merges(&log_merge, vec![(1i64.into(), 2000, true)]);
        assert_eq!(jobs.len(), 1);

        Ok(())
    }

    #[test]
    fn test_log_merge_scheduling_not_enough_segments_merge() -> anyhow::Result<()> {
        let log_merge = LogMergeSettings {
            min_number_of_segments: 3,
            ..Default::default()
        };
        let jobs = plan_merges(&log_merge, vec![(1i64.into(), 50, false), (2i64.into(), 50, false)]);
        assert!(jobs.is_empty());

        Ok(())
    }

    #[test]
    fn test_log_merge_scheduling_same_size_segments() -> anyhow::Result<()> {
        let log_merge = LogMergeSettings {
            min_number_of_segments: 3,
            ..Default::default()
        };
        let jobs = plan_merges(
            &log_merge,
            vec![
                (1i64.into(), 50, false),
                (2i64.into(), 50, false),
                (3i64.into(), 50, false),
            ],
        );

        // all segments have been scheduled for merge in a single job
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].len(), 3);

        Ok(())
    }

    #[test]
    fn test_log_merge_scheduling_all_buckets() -> anyhow::Result<()> {
        let log_merge = LogMergeSettings {
            min_number_of_segments: 2,
            top_bucket_max_records: 1000,
            bottom_bucket_threshold: 50,
            bucket_size_log: 1.0,
        };

        let jobs = plan_merges(
            &log_merge,
            vec![
                (7i64.into(), 1001, false), // exceeds the max segment size, won't appear
                (3i64.into(), 1000, false), // log2(1000) = ~9.97 -- will mark the top bucket
                (12i64.into(), 501, false), // last element in top bucket
                (13i64.into(), 500, false), // just below top bucket
                (11i64.into(), 249, false), // top - 2
                (9i64.into(), 125, false),  // top - 2
                (5i64.into(), 124, false),  // bottom + 1
                (4i64.into(), 63, false),   // bottom + 1
                (6i64.into(), 62, false),   // first in bottom bucket
                (10i64.into(), 51, false),  // just above bottom_bucket_threshold
                (1i64.into(), 50, false),   // bottom bucket
                (2i64.into(), 10, false),   // bottom bucket
                (8i64.into(), 20, false),   // bottom bucket
            ],
        );

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
        assert_eq!(
            jobs[3],
            vec![6i64.into(), 10i64.into(), 1i64.into(), 2i64.into(), 8i64.into()]
        );

        // Not merged
        // 1001 -> top_bucket_max_records
        // 500  -> min_number_of_segments

        Ok(())
    }
}
