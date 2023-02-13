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

use clap::Args;

use crate::threshold::Threshold;

use super::strategy::Strategy;

/// A structure containing all shard balancing settings.
#[derive(Args)]
pub struct Settings {
    /// The shard balancing strategy.
    #[arg(short, long, value_enum, default_value = "optimal")]
    pub(crate) strategy: Strategy,
    /// The value that drives the shard balancing strategy.
    #[arg(short, long)]
    pub(crate) tolerance: Threshold,
    /// The maximum number of shards per node.
    #[arg(long, env = "XXX")]
    pub(crate) shard_limit: u64,
}
