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
use nucliadb_core::NodeResult;
use nucliadb_node::settings::providers::env::EnvSettingsProvider;
use nucliadb_node::settings::providers::SettingsProvider;
use nucliadb_node::unified::replication;
use nucliadb_node::unified::replication::connect_to_primary_and_replicate;
use nucliadb_node::unified::service::run_server;
use nucliadb_node::unified::NodeRole;
use nucliadb_node::utils;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tokio::signal::unix::SignalKind;
use tokio::signal::{ctrl_c, unix};

#[tokio::main]
async fn main() -> NodeResult<()> {
    eprintln!("NucliaDB Node starting...");
    let settings = Arc::new(EnvSettingsProvider::generate_settings()?);

    if !settings.shards_path().exists() {
        // create folder if it does not exist
        std::fs::create_dir_all(settings.shards_path().as_path()).unwrap();
    }

    let shard_manager = Arc::new(Mutex::new(
        nucliadb_node::unified::shards::ShardManager::new(PathBuf::from(settings.shards_path())),
    ));
    shard_manager
        .lock()
        .unwrap()
        .load()
        .expect("Failed to load shards");

    let host_key_path = settings.host_key_path();
    let node_id = utils::read_or_create_host_key(host_key_path)?;

    let mut primary_replicator = None;
    if settings.node_role() == NodeRole::Primary {
        primary_replicator = Some(Arc::new(Mutex::new(replication::PrimaryReplicator::new(
            shard_manager.clone(),
        ))));
    }

    let servicer_task = tokio::spawn(run_server(
        settings.clone(),
        settings.listen_address(),
        shard_manager.clone(),
        primary_replicator,
    ));

    let mut replicate_task = None;

    if settings.node_role() == NodeRole::Secondary {
        eprintln!("Replicating from {}...", settings.primary_address());
        replicate_task = Some(tokio::spawn(connect_to_primary_and_replicate(
            settings.primary_address().to_string(),
            node_id.to_string(),
            Arc::new(Mutex::new(replication::SecondaryReplicator::new(
                shard_manager.clone(),
            ))),
            settings,
        )));
    }

    eprintln!("Running");

    wait_for_sigkill().await?;

    eprintln!("Shutting down NucliaDB Writer Node...");
    if let Some(task) = replicate_task {
        task.abort();
        let _ = task.await?;
    }
    servicer_task.abort();
    let _ = servicer_task.await?;

    Ok(())
}

async fn wait_for_sigkill() -> NodeResult<()> {
    let mut sigterm = unix::signal(SignalKind::terminate())?;
    let mut sigquit = unix::signal(SignalKind::quit())?;

    tokio::select! {
        _ = sigterm.recv() => println!("Terminating on SIGTERM"),
        _ = sigquit.recv() => println!("Terminating on SIGQUIT"),
        _ = ctrl_c() => println!("Terminating on ctrl-c"),
    }

    Ok(())
}
