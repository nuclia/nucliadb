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

#[allow(unused)]
mod distance;
mod graph_arena;
mod graph_disk;
mod graph_elems;
mod heuristics;
mod memory_processes;
mod query;
mod query_delete;
mod query_find_labels;
mod query_insert;
mod query_post_search;
mod query_search;
mod query_writer_search;
mod read_index;
pub mod reader;
#[cfg(test)]
mod tests;
mod trace_utils;
mod utils;
mod write_index;
pub mod writer;
