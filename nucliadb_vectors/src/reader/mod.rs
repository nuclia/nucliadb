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

use std::fmt::Debug;
use std::path::Path;

use tracing::*;

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::Query;
use crate::query_post_search::{PostSearchQuery, PostSearchValue};
use crate::query_search::{SearchQuery, SearchValue};

pub struct Reader {
    index: Index,
}

impl Debug for Reader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorReader")
            .field("index", &self.index)
            .finish()
    }
}

impl Reader {
    pub fn new(path: &str) -> Reader {
        Reader {
            index: Index::reader(Path::new(path)),
        }
    }
    pub fn search(
        &self,
        elem: Vec<f32>,
        labels: Vec<String>,
        no_results: usize,
    ) -> Vec<(String, f32)> {
        let SearchValue { neighbours } = SearchQuery {
            elem: Vector::from(elem),
            k_neighbours: hnsw_params::k_neighbours(),
            index: &self.index,
            with_filter: &labels,
        }
        .run();
        debug!("Neighbours {}", neighbours.len());
        let PostSearchValue { filtered } = PostSearchQuery {
            up_to: no_results,
            pre_filter: neighbours,
            with_filter: labels,
            index: &self.index,
        }
        .run();
        filtered
    }
    pub fn reload(&mut self) {
        self.index.reload();
    }
    pub fn no_vectors(&self) -> usize {
        self.index.no_nodes() as usize
    }
    pub fn stats(&self) -> Stats {
        self.index.stats()
    }
    pub fn keys(&self) -> Vec<String> {
        self.index.get_keys()
    }
}
