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

use clap::Parser;
use eyre::{eyre, Result};
use nucliadb_shard_balancer::balancer::{BalanceSettings, Balancer};
use nucliadb_shard_balancer::node::Node;
use nucliadb_shard_balancer::shard::ShardIndex;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Opt {
    url: String,
    #[arg(short, long)]
    dry_run: bool,
    #[command(flatten)]
    balance_settings: BalanceSettings,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .map_err(|e| eyre!(e))?;

    let opt = Opt::parse();

    let nodes = Node::from_api(&opt.url).await?;
    tracing::debug!("Nodes: {nodes:?}");

    let shard_index = ShardIndex::from_api(&opt.url, &nodes).await?;
    tracing::debug!("Shard index: {shard_index:?}");

    let balancer = Balancer::new(opt.balance_settings);

    // Execute shard balancing one by one to handle
    // full node and downtime cases.
    for shard_cutover in balancer.balance_shards(nodes, &shard_index) {
        tracing::debug!("Shard cutover: {shard_cutover:?}");

        if !opt.dry_run {
            shard_cutover.execute().await?;
        }
    }

    Ok(())
}
