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

use std::collections::{HashMap, VecDeque};

use itertools::Itertools;

use crate::shards::ShardId;

#[derive(PartialEq, Eq)]
pub struct MergeRequest {
    pub shard_id: ShardId,
    pub priority: MergePriority,
}

impl PartialOrd for MergeRequest {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeRequest {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.priority.cmp(&other.priority)
    }
}

#[derive(Debug, Default, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub enum MergePriority {
    WhenFree = 0,
    #[default]
    Low,
    High,
}

const MERGE_PRIORITIES_COUNT: usize = 3;

pub struct WorkQueue {
    queues: HashMap<MergePriority, VecDeque<MergeRequest>>,
}

// `WorkQueue` implementation uses a hash map to decouple the code from
// `MergePriority`
//
// As the amount of priorities is small, we can consider hash map operations
// take constant time. Using a hash map decouples the code from hardcoding
// priorities in our methods.
impl WorkQueue {
    pub fn new() -> Self {
        let mut queues = HashMap::with_capacity(MERGE_PRIORITIES_COUNT);
        queues.insert(MergePriority::WhenFree, VecDeque::new());
        queues.insert(MergePriority::Low, VecDeque::new());
        queues.insert(MergePriority::High, VecDeque::new());

        Self {
            queues,
        }
    }

    /// Pushes an item to the queue
    pub fn push(&mut self, item: MergeRequest) {
        let queue = self.queues.get_mut(&item.priority).expect("Priority queue must exist");
        queue.push_back(item);
    }

    /// Removes the greatest item from the queue and returns it, or `None` if
    /// it's empty
    pub fn pop(&mut self) -> Option<MergeRequest> {
        for (_, queue) in self.queues.iter_mut().sorted().rev() {
            if let Some(item) = queue.pop_front() {
                return Some(item);
            }
        }
        None
    }

    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.queues.values().fold(0, |acc, queue| acc + queue.len())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_merge_request_priorities() {
        let urgent = MergeRequest {
            shard_id: "urgent".to_string(),
            priority: MergePriority::High,
        };
        let deferrable = MergeRequest {
            shard_id: "deferrable".to_string(),
            priority: MergePriority::Low,
        };
        let not_important = MergeRequest {
            shard_id: "not-important".to_string(),
            priority: MergePriority::WhenFree,
        };

        assert!(urgent > deferrable);
        assert!(urgent > not_important);
        assert!(deferrable < urgent);
        assert!(deferrable > not_important);
        assert!(not_important < urgent);
        assert!(not_important < deferrable);
    }

    #[test]
    fn test_merge_priority_variants() {
        // Ensure the hardcoded constant remains correct
        match MergePriority::WhenFree {
            MergePriority::WhenFree => {}
            MergePriority::Low => {}
            MergePriority::High => {}
        }
        assert_eq!(MERGE_PRIORITIES_COUNT, 3);
    }

    #[test]
    fn test_work_queues_priorities() {
        let mut queue = WorkQueue::new();
        assert_eq!(queue.len(), 0);
        assert!(queue.pop().is_none());

        queue.push(MergeRequest {
            shard_id: "A".to_string(),
            priority: MergePriority::WhenFree,
        });
        queue.push(MergeRequest {
            shard_id: "B".to_string(),
            priority: MergePriority::High,
        });
        queue.push(MergeRequest {
            shard_id: "C".to_string(),
            priority: MergePriority::Low,
        });
        assert_eq!(queue.len(), 3);

        let item = queue.pop().unwrap();
        assert_eq!(item.priority, MergePriority::High);
        assert_eq!(item.shard_id, "B");

        let item = queue.pop().unwrap();
        assert_eq!(item.priority, MergePriority::Low);
        assert_eq!(item.shard_id, "C");

        let item = queue.pop().unwrap();
        assert_eq!(item.priority, MergePriority::WhenFree);
        assert_eq!(item.shard_id, "A");

        assert!(queue.pop().is_none());

        assert_eq!(queue.len(), 0);
    }
}
