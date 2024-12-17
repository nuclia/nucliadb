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

use std::hash::{DefaultHasher, Hash as _, Hasher as _};
use uuid::Uuid;

enum SearcherNode {
    This,
    Remote(String),
}

trait ListNodes {
    fn list_nodes(&self) -> Vec<String>;
    fn this_node(&self) -> String;
}

pub struct OneNode;
impl ListNodes for OneNode {
    fn list_nodes(&self) -> Vec<String> {
        vec![self.this_node()]
    }

    fn this_node(&self) -> String {
        "single".to_string()
    }
}

#[derive(Hash)]
struct Scored<'a>(&'a Uuid, &'a String);

impl<'a> Scored<'a> {
    fn score(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.hash(&mut s);
        s.finish()
    }
}

pub struct ShardSelector<T: ListNodes> {
    nodes: T,
    num_replicas: usize,
}

impl<T: ListNodes> ShardSelector<T> {
    pub fn new(nodes: T, num_replicas: usize) -> Self {
        Self {
            nodes,
            num_replicas,
        }
    }

    pub fn select_shards(&self, all_shards: Vec<Uuid>) -> Vec<Uuid> {
        let this_node = self.nodes.this_node();
        all_shards.into_iter().filter(move |shard| self._nodes_for_shard(shard).contains(&this_node)).collect()
    }

    pub fn nodes_for_shard(&self, shard: &Uuid) -> Vec<SearcherNode> {
        let nodes = self._nodes_for_shard(shard);
        // See if some of this nodes are me, and put them first
        let mut searcher_nodes = Vec::new();
        let mut has_this_node = false;
        for node in nodes {
            if node == self.nodes.this_node() {
                has_this_node = true;
            } else {
                searcher_nodes.push(SearcherNode::Remote(node));
            }
        }
        if has_this_node {
            searcher_nodes.insert(0, SearcherNode::This);
        }
        searcher_nodes
    }

    fn _nodes_for_shard(&self, shard: &Uuid) -> Vec<String> {
        // Get the nodes that should store this shard
        let mut nodes = self.nodes.list_nodes();
        nodes.sort_by_cached_key(|s| Scored(shard, s).score());
        nodes.truncate(self.num_replicas);

        nodes
    }
}
