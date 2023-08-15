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

use nucliadb_index::index;
use nucliadb_node::unified::replication::connect_to_primary_and_replicate;
use nucliadb_node::unified::service::run_server;
use nucliadb_node::unified::{node::ShardWriter, replication};

use std::path::PathBuf;
use std::{
    net::TcpListener,
    sync::{Arc, Mutex},
};

fn port_is_available(port: u16) -> bool {
    match TcpListener::bind(("127.0.0.1", port)) {
        Ok(_) => true,
        Err(_) => false,
    }
}

fn get_available_port() -> Option<u16> {
    (1025..65535).find(|port| port_is_available(*port))
}

#[tokio::test]
async fn test_replicate() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = tempfile::tempdir().unwrap();
    let port = get_available_port().unwrap();

    let shard_manager = Arc::new(Mutex::new(
        nucliadb_node::unified::shards::ShardManager::new(PathBuf::from(workspace.path())),
    ));
    let primary_replicator = Arc::new(Mutex::new(replication::PrimaryReplicator::new(
        shard_manager.clone(),
    )));
    let secondary_replicator = Arc::new(Mutex::new(replication::SecondaryReplicator::new(
        shard_manager.clone(),
    )));
    let mut shard_writer = ShardWriter::new(shard_manager.clone(), primary_replicator.clone());

    let address_raw = "127.0.0.1:".to_string() + &port.to_string();
    let address = address_raw.parse();
    if address.is_err() {
        let err_str = format!("Invalid address: {}", address_raw);
        return Err(err_str.into());
    }
    let servicer_task = tokio::spawn(run_server(
        address.unwrap(),
        shard_manager.clone(),
        Some(primary_replicator.clone()),
    ));

    // wait for server to start
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    let replicate_task = tokio::spawn(connect_to_primary_and_replicate(
        "http://".to_string() + &address_raw,
        "node_id".to_string(),
        secondary_replicator.clone(),
    ));

    shard_manager.lock().unwrap().create_shard("shard_id1")?;
    shard_manager.lock().unwrap().create_shard("shard_id2")?;
    assert_eq!(
        shard_writer.index_resource(
            "shard_id1",
            index::ResourceData {
                id: "resource_id1".to_string()
            }
        )?,
        1
    );
    assert_eq!(
        shard_writer.index_resource(
            "shard_id1",
            index::ResourceData {
                id: "resource_id2".to_string()
            }
        )?,
        2
    );
    assert_eq!(
        shard_writer.index_resource(
            "shard_id2",
            index::ResourceData {
                id: "resource_id3".to_string()
            }
        )?,
        1
    );
    assert_eq!(
        shard_writer.index_resource(
            "shard_id2",
            index::ResourceData {
                id: "resource_id4".to_string()
            }
        )?,
        2
    );

    assert_eq!(servicer_task.is_finished(), false);
    assert_eq!(replicate_task.is_finished(), false);

    assert_eq!(
        secondary_replicator
            .lock()
            .unwrap()
            .get_position("shard_id1"),
        2
    );
    assert_eq!(
        secondary_replicator
            .lock()
            .unwrap()
            .get_position("shard_id2"),
        2
    );

    servicer_task.abort();
    replicate_task.abort();
    let _ = tokio::join!(servicer_task, replicate_task);

    Ok(())
}
