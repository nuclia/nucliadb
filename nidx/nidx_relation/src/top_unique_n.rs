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

use std::collections::HashMap;

/// Top unique N
///
/// Maintain the top set of unique keys with greatest scores.
pub struct TopUniqueN<K> {
    elements: HashMap<K, f32>,
    top_n: usize,
    threshold: f32,
}

impl<K> TopUniqueN<K>
where
    K: Eq + std::hash::Hash + std::fmt::Debug,
{
    pub fn new(top_n: usize) -> Self {
        Self {
            top_n,
            elements: HashMap::with_capacity(2 * top_n),
            threshold: f32::NEG_INFINITY,
        }
    }

    pub fn insert(&mut self, key: K, score: f32) {
        if score < self.threshold {
            return;
        }

        if self.elements.len() == self.elements.capacity() {
            let lowest_score = self.truncate_top_n();
            self.threshold = lowest_score;
        }

        self.elements
            .entry(key)
            .and_modify(|s| {
                if score > *s {
                    *s = score
                }
            })
            .or_insert(score);
    }

    // Truncate the current set of element to N leaving only the top-scoring
    // elements. Return the smallest score across the top.
    fn truncate_top_n(&mut self) -> f32 {
        let mut vec = Vec::from_iter(self.elements.drain());
        vec.sort_unstable_by(|a, b| a.1.total_cmp(&b.1).reverse());
        vec.truncate(self.top_n);
        let lowest_score = vec.last().map(|(_, score)| *score).unwrap_or(f32::NEG_INFINITY);

        self.elements.extend(vec);

        lowest_score
    }

    pub fn into_sorted_vec(self) -> Vec<(K, f32)> {
        let mut vec = Vec::from_iter(self.elements);
        vec.sort_by(|a, b| a.1.total_cmp(&b.1).reverse());
        vec.truncate(self.top_n);
        vec
    }

    pub fn merge(&mut self, other: Self) {
        for (key, score) in other.elements.into_iter() {
            self.insert(key, score);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_top_n() {
        let mut top = TopUniqueN::new(2);
        top.insert("A", 1.0);
        top.insert("B", 3.0);
        top.insert("C", 2.0);
        top.insert("D", 4.0);
        top.insert("E", -1.0);

        let r = top.into_sorted_vec();
        assert_eq!(r.len(), 2);
        let r: HashMap<_, _> = HashMap::from_iter(r);
        let expected = HashMap::from_iter([("B", 3.0), ("D", 4.0)]);
        assert_eq!(r, expected);
    }

    /// Validate inserting more than it's capacity, values are truncated to N.
    #[test]
    fn test_internal_truncate() {
        const N: usize = 2;
        let mut top = TopUniqueN::new(N);

        // capacity is at least 2 * N + 1, but in reality, it's usually more
        let actual_capacity = top.elements.capacity();
        assert!(actual_capacity >= 2 * N);

        let mut key_id = 0;
        let mut key_generator = std::iter::repeat_with(|| {
            let key = key_id.to_string();
            key_id += 1;
            key
        });

        while top.elements.len() < top.elements.capacity() {
            let key = key_generator.next().unwrap();
            top.insert(key, 1.0);
        }
        assert_eq!(top.elements.len(), top.elements.capacity());
        assert!(top.threshold < 0.0);

        // this insert would overflow the capacity, but it truncates the internal values and don't
        // increase it
        top.insert("A".to_string(), 1.0);
        assert_eq!(top.elements.capacity(), actual_capacity);
        assert_eq!(top.elements.len(), N + 1);
        assert_eq!(top.threshold, 1.0);
    }

    #[test]
    fn test_merge() {
        let mut top_a = TopUniqueN::new(4);
        top_a.insert("A1", 1.0);
        top_a.insert("A2", 3.0);

        let mut top_b = TopUniqueN::new(3);
        top_b.insert("B1", 1.0);
        top_b.insert("B2", 3.0);
        top_b.insert("B3", 4.0);
        top_b.insert("B4", 2.0);

        top_a.merge(top_b);

        let r = top_a.into_sorted_vec();
        assert_eq!(r.len(), 4);

        let r: HashMap<_, _> = HashMap::from_iter(r);
        let expected = HashMap::from_iter([("A2", 3.0), ("B2", 3.0), ("B3", 4.0), ("B4", 2.0)]);
        assert_eq!(r, expected);
    }
}
