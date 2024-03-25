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

use crate::shards::ShardId;

pub struct MergeRequest {
    pub shard_id: ShardId,
    pub priority: MergePriority,
    pub waiter: MergeWaiter,
}

#[derive(Copy, Clone, Default)]
pub enum MergeWaiter {
    #[default]
    None,
    Async,
}

#[derive(Copy, Clone, Default, Hash, PartialEq, Eq)]
pub enum MergePriority {
    WhenFree,
    #[default]
    Low,
    High,
}

pub const MERGE_PRIORITIES: [MergePriority; 3] = [MergePriority::High, MergePriority::Low, MergePriority::WhenFree];
