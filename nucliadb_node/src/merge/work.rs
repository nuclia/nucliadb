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

use super::MergePriority;
use super::MERGE_PRIORITIES;

/// Work queue structure for merge scheduler. It serves as a priority queue with
/// deduplication of elements
pub struct WorkQueue<T> {
    queues: HashMap<MergePriority, VecDeque<T>>,
}

// `WorkQueue` implementation uses a hash map to decouple the code from
// `MergePriority`
//
// As the amount of priorities is small, we can consider hash map operations
// take constant time. Using a hash map decouples the code from hardcoding
// priorities in our methods.
impl<T: PartialEq> WorkQueue<T> {
    pub fn new() -> Self {
        let mut queues = HashMap::with_capacity(MERGE_PRIORITIES.len());
        for priority in MERGE_PRIORITIES {
            queues.insert(priority, VecDeque::new());
        }

        Self {
            queues,
        }
    }

    /// Pushes an item to the queue if it wasn't there yet with an specific
    /// priority. Deduplication only occurrs in the requested priority
    pub fn push(&mut self, item: T, priority: MergePriority) {
        let queue = self.queues.get_mut(&priority).expect("Priority queue must exist");

        if !queue.contains(&item) {
            queue.push_back(item);
        }
    }

    /// Removes the greatest item from the queue and returns it, or `None` if
    /// it's empty
    pub fn pop(&mut self) -> Option<T> {
        for priority in MERGE_PRIORITIES {
            let queue = self.queues.get_mut(&priority).unwrap();
            let item = queue.pop_front();
            if item.is_some() {
                return item;
            }
        }
        None
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.queues.values().fold(0, |acc, queue| acc + queue.len())
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_work_queues_priorities() {
        let mut queue = WorkQueue::new();
        assert_eq!(queue.len(), 0);
        assert!(queue.pop().is_none());

        queue.push("A", MergePriority::WhenFree);
        queue.push("B", MergePriority::High);
        queue.push("C", MergePriority::Low);
        assert_eq!(queue.len(), 3);

        let item = queue.pop().unwrap();
        assert_eq!(item, "B");

        let item = queue.pop().unwrap();
        assert_eq!(item, "C");

        let item = queue.pop().unwrap();
        assert_eq!(item, "A");

        assert!(queue.pop().is_none());

        assert_eq!(queue.len(), 0);
    }

    #[test]
    fn test_work_queue_dedup_push() {
        let mut queue = WorkQueue::new();
        let item = "item";
        queue.push(item, MergePriority::default());
        queue.push(item, MergePriority::default());
        assert_eq!(queue.len(), 1);
    }
}
