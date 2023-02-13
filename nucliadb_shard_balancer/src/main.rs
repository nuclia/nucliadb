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
use futures::future;
use tracing_subscriber::EnvFilter;
use url::Url;

use nucliadb_shard_balancer::balancer::{Balancer, Settings, ShardCutover};
use nucliadb_shard_balancer::node::Node;

#[derive(Parser)]
struct Opt {
    url: Url,
    #[command(flatten)]
    settings: Settings,
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .map_err(|e| eyre!(e))?;

    let opt = Opt::parse();

    let nodes = Node::fetch_all(opt.url).await?;
    tracing::debug!("Fetched nodes: {nodes:?}");

    let balancer = Balancer::new(opt.settings);
    let balance_tasks = balancer
        .balance_shards(&nodes)
        .into_iter()
        .map(ShardCutover::execute)
        .collect::<Vec<_>>();

    futures::future::join_all(balance_tasks).await;

    Ok(())
}
