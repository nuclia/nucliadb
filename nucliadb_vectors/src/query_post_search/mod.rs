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

use rayon::prelude::*;

use crate::index::*;
use crate::memory_system::elements::*;
use crate::query::Query;
#[derive(Clone, Default)]
pub struct PostSearchValue {
    pub filtered: Vec<(String, f32)>,
}

pub struct PostSearchQuery<'a> {
    pub up_to: usize,
    pub pre_filter: Vec<(Node, f32)>,
    pub with_filter: Vec<String>,
    pub index: &'a Index,
}

impl<'a> Query for PostSearchQuery<'a> {
    type Output = PostSearchValue;

    fn run(&mut self) -> Self::Output {
        let pre_filter = std::mem::take(&mut self.pre_filter);
        let mut filtered: Vec<_> = pre_filter
            .into_par_iter()
            .map(|(node, dist)| (self.index.get_node_key(node), dist))
            .collect();
        filtered.truncate(self.up_to);
        PostSearchValue { filtered }
    }
}
