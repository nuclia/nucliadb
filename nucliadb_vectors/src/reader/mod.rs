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

use crate::graph_disk::*;
use crate::graph_elems::{GraphVector, HNSWParams};
use crate::query::Query;
use crate::query_find_labels::FindLabelsQuery;
use crate::query_post_search::{PostSearchQuery, PostSearchValue};
use crate::query_search::{SearchQuery, SearchValue};
use crate::read_index::*;

pub struct Reader {
    index: LockReader,
    params: HNSWParams,
    disk: LockDisk,
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
        let disk = Disk::start(Path::new(path));
        let index = ReadIndex::new(&disk);
        Reader {
            disk: disk.into(),
            index: index.into(),
            params: HNSWParams::default(),
        }
    }

    pub fn search(
        &self,
        elem: Vec<f32>,
        labels: Vec<String>,
        no_results: usize,
    ) -> Vec<(String, f32)> {
        let is_filtered_search = !labels.is_empty();
        let label_analysis = FindLabelsQuery {
            labels,
            disk: &self.disk,
        }
        .run();

        let result = if is_filtered_search && label_analysis.min_reached < no_results {
            Vec::with_capacity(0)
        } else {
            let SearchValue { neighbours } = SearchQuery {
                elem: GraphVector::from(elem),
                k_neighbours: self.params.k_neighbours,
                index: &self.index,
                disk: &self.disk,
            }
            .run();
            debug!("Neighbours {}", neighbours.len());
            let PostSearchValue { filtered } = PostSearchQuery {
                pre_filter: neighbours,
                with_filter: label_analysis.found,
                index: &self.index,
            }
            .run();
            filtered
        };
        result
    }
    pub fn reload(&self) {
        let current_version = self.index.get_version_number();
        let disk_version = self.disk.get_version_number();
        if current_version != disk_version {
            self.index.reload(&self.disk);
        }
    }

    pub fn no_neighbours(&self) -> usize {
        self.params.k_neighbours
    }
    pub fn no_vectors(&self) -> usize {
        self.disk.no_nodes()
    }
}
