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
//

use crate::{metadata::SegmentId, settings::VectorMergeSettings};

/// [`VectorMergeStrategy`] implements a merging strategy designed for the HNSW
/// index with the following constraints:
/// - Small segments should be merged often to keep number of segments low
/// - Big segments should be merged seldom since merging is very slow
/// - It's fast to merge a single big segment with multiple slow ones (appending)
///
/// With this in mind, the idea is to merge all small segments together until they
/// reach a certain size and then avoid merging the bigger segments until there are
/// too many of them or we are forced due to deletions.
pub fn plan_merges(settings: &VectorMergeSettings, segments: Vec<(SegmentId, i64, bool)>) -> Vec<Vec<SegmentId>> {
    let mut merges = Vec::new();

    // Segments come sorted for largest to smallest, start processing big segments
    let (big, small): (Vec<_>, Vec<_>) = segments
        .iter()
        .partition(|(_, records, _)| *records > settings.small_segment_threshold as i64);

    // Merge big segments if there are many of them or we are forced
    let mut forced = false;
    let mut buffer_records = 0;
    let mut merge_buffer = Vec::new();
    for (segment_id, records, force) in big.iter() {
        forced |= force;
        buffer_records += records;
        merge_buffer.push(*segment_id);

        if buffer_records > settings.max_segment_size as i64 {
            if merge_buffer.len() >= settings.min_number_of_segments || forced {
                merges.push(std::mem::take(&mut merge_buffer));
            }
            merge_buffer.clear();
            forced = false;
            buffer_records = 0;
        }
    }
    if merge_buffer.len() >= settings.min_number_of_segments || forced {
        merges.push(merge_buffer);
    }

    // Merge small segments
    let mut forced = false;
    let mut buffer_records = 0;
    let mut merge_buffer = Vec::new();
    for (segment_id, records, force) in small.iter().rev() {
        forced |= force;
        buffer_records += records;
        merge_buffer.push(*segment_id);

        if buffer_records > settings.small_segment_threshold as i64 {
            if merge_buffer.len() > 1 || forced {
                merges.push(std::mem::take(&mut merge_buffer));
            }
            merge_buffer.clear();
            forced = false;
            buffer_records = 0;
        }
    }
    if merge_buffer.len() > 1 || forced {
        merges.push(merge_buffer);
    }

    merges
}

#[cfg(test)]
mod tests {
    use crate::{scheduler::vector_merge::plan_merges, settings::VectorMergeSettings};

    #[test]
    fn test_vector_merge_scheduling_forced_merge() -> anyhow::Result<()> {
        let settings = VectorMergeSettings {
            min_number_of_segments: 3,
            max_segment_size: 1000,
            small_segment_threshold: 10,
        };

        let jobs = plan_merges(&settings, vec![(1i64.into(), 50, false)]);
        assert_eq!(jobs.len(), 0);

        let jobs = plan_merges(&settings, vec![(1i64.into(), 50, true)]);
        assert_eq!(jobs.len(), 1);

        let jobs = plan_merges(&settings, vec![(1i64.into(), 5000, false)]);
        assert_eq!(jobs.len(), 0);

        let jobs = plan_merges(&settings, vec![(1i64.into(), 5000, true)]);
        assert_eq!(jobs.len(), 1);

        Ok(())
    }

    #[test]
    fn test_vector_merge_scheduling_not_enough_segments_merge() -> anyhow::Result<()> {
        let settings = VectorMergeSettings {
            min_number_of_segments: 3,
            max_segment_size: 1000,
            small_segment_threshold: 10,
        };
        let jobs = plan_merges(&settings, vec![(1i64.into(), 50, false), (2i64.into(), 50, false)]);
        assert!(jobs.is_empty());

        let jobs = plan_merges(&settings, vec![(1i64.into(), 5, false), (2i64.into(), 5, false)]);
        assert_eq!(jobs.len(), 1);

        Ok(())
    }

    #[test]
    fn test_vector_merge_scheduling_all_buckets() -> anyhow::Result<()> {
        let settings = VectorMergeSettings {
            min_number_of_segments: 3,
            max_segment_size: 10000,
            small_segment_threshold: 100,
        };

        let jobs = plan_merges(
            &settings,
            vec![
                (1i64.into(), 10001, false), // Too big to merge
                (2i64.into(), 5500, false),  // Big merged together up to merge size
                (3i64.into(), 3000, false),
                (4i64.into(), 2000, false),
                (5i64.into(), 1000, false),
                (6i64.into(), 40, false), // All smalls merged in batches of small_segment_threshold
                (7i64.into(), 38, false),
                (8i64.into(), 36, false),
                (9i64.into(), 24, false),
                (10i64.into(), 22, false),
                (11i64.into(), 20, false),
                (12i64.into(), 10, false),
            ],
        );

        assert_eq!(jobs.len(), 3);

        // For test simplicity, we rely on jobs being created in
        // top-to-bottom order. Feel free to change this is the algorithm
        // changes

        // Big segments
        assert_eq!(jobs[0], vec![2i64.into(), 3i64.into(), 4i64.into()]);
        // First batch of small segments
        assert_eq!(
            jobs[1],
            vec![12i64.into(), 11i64.into(), 10i64.into(), 9i64.into(), 8i64.into()]
        );
        // Second batch of small segments
        assert_eq!(jobs[2], vec![7i64.into(), 6i64.into()]);

        // Not merged
        // 10001 -> top_bucket_max_records

        Ok(())
    }
}
