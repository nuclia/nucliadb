use std::fs;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use nucliadb_swim::prelude::{
    ArtilleryError, ArtilleryMember, ArtilleryMemberEvent, ArtilleryMemberState,
    Cluster as ArtilleryCluster, ClusterConfig as ArtilleryClusterConfig,
};
use serde::Serialize;
use tokio::sync::watch;
use tokio::time::timeout;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::error::{ClusterError, ClusterResult};

/// The ID that makes the cluster unique.
const CLUSTER_ID: &str = "nucliadb-cluster";

const CLUSTER_EVENT_TIMEOUT: Duration = Duration::from_millis(200);

pub fn read_host_key(host_key_path: &Path) -> ClusterResult<Uuid> {
    let host_key_contents =
        fs::read(host_key_path).map_err(|err| ClusterError::ReadHostKeyError {
            message: err.to_string(),
        })?;

    let host_key = Uuid::from_slice(host_key_contents.as_slice()).map_err(|err| {
        ClusterError::ReadHostKeyError {
            message: err.to_string(),
        }
    })?;

    Ok(host_key)
}

/// Reads the key that makes a node unique from the given file.
/// If the file does not exist, it generates an ID and writes it to the file
/// so that it can be reused on reboot.
pub fn read_or_create_host_key(host_key_path: &Path) -> ClusterResult<Uuid> {
    let host_key;

    if host_key_path.exists() {
        host_key = read_host_key(host_key_path)?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Read existing host key.");
    } else {
        if let Some(dir) = host_key_path.parent() {
            if !dir.exists() {
                fs::create_dir_all(dir).map_err(|err| ClusterError::WriteHostKeyError {
                    message: err.to_string(),
                })?;
            }
        }
        host_key = Uuid::new_v4();
        fs::write(host_key_path, host_key.as_bytes()).map_err(|err| {
            ClusterError::WriteHostKeyError {
                message: err.to_string(),
            }
        })?;
        info!(host_key=?host_key, host_key_path=?host_key_path, "Create new host key.");
    }

    Ok(host_key)
}

/// A member information.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct Member {
    /// An ID that makes a member unique.
    pub node_id: String,

    /// Listen address.
    pub listen_addr: SocketAddr,

    // Type of node 'l': node reader 'e': node writer 'r': reader 'w': writer
    pub node_type: char,
    /// If true, it means self.
    pub is_self: bool,
}

/// This is an implementation of a cluster using the SWIM protocol.
pub struct Cluster {
    /// A socket address that represents itself.
    pub listen_addr: SocketAddr,

    /// The actual cluster that implement the SWIM protocol.
    artillery_cluster: ArtilleryCluster,

    /// A receiver(channel) for exchanging members in a cluster.
    members: watch::Receiver<Vec<Member>>,

    /// A stop flag of cluster monitoring task.
    /// Once the cluster is created, a task to monitor cluster events will be started.
    /// Nodes do not need to be monitored for events once they are detached from the cluster.
    /// You need to update this value to get out of the task loop.
    stop: Arc<AtomicBool>,
}

impl Cluster {
    /// Create a cluster given a host key and a listen address.
    /// When a cluster is created, the thread that monitors cluster events
    /// will be started at the same time.
    pub fn new(
        node_id: String,
        listen_addr: SocketAddr,
        node_type: char,
        ping_timeout: u64,
        ping_interval: u64,
    ) -> ClusterResult<Self> {
        info!( node_id=?node_id, listen_addr=?listen_addr, "Create new cluster.");
        let config = ArtilleryClusterConfig {
            cluster_key: CLUSTER_ID.as_bytes().to_vec(),
            listen_addr,
            ping_timeout: Duration::from_secs(ping_timeout),
            ping_interval: Duration::from_secs(ping_interval),
            ..Default::default()
        };

        let (artillery_cluster, swim_event_rx) =
            ArtilleryCluster::create_and_start(node_id.clone(), node_type, config).map_err(
                |err| match err {
                    ArtilleryError::Io(io_err) => ClusterError::UDPPortBindingError {
                        port: listen_addr.port(),
                        message: io_err.to_string(),
                    },
                    _ => ClusterError::CreateClusterError {
                        message: err.to_string(),
                    },
                },
            )?;

        let (members_sender, members_receiver) = watch::channel(Vec::new());

        // Create cluster.
        let cluster = Cluster {
            listen_addr,
            artillery_cluster,
            members: members_receiver,
            stop: Arc::new(AtomicBool::new(false)),
        };

        // Add itself as the initial member of the cluster.
        let member = Member {
            node_id: node_id.clone(),
            listen_addr,
            node_type,
            is_self: true,
        };
        let initial_members: Vec<Member> = vec![member];
        if members_sender.send(initial_members).is_err() {
            error!("Failed to add itself as the initial member of the cluster.");
        }

        // Prepare to start a task that will monitor cluster events.
        let task_listen_addr = cluster.listen_addr;
        let task_stop = cluster.stop.clone();

        // Start to monitor the cluster events.
        tokio::task::spawn_blocking(move || {
            loop {
                match swim_event_rx.recv_timeout(CLUSTER_EVENT_TIMEOUT) {
                    Ok((artillery_members, artillery_member_event)) => {
                        log_artillery_event(node_id.clone(), artillery_member_event);
                        let updated_memberlist: Vec<Member> = artillery_members
                            .into_iter()
                            .filter(|member| match member.state() {
                                ArtilleryMemberState::Alive | ArtilleryMemberState::Suspect => true,
                                ArtilleryMemberState::Down | ArtilleryMemberState::Left => false,
                            })
                            .map(|member| convert_member(member, task_listen_addr))
                            .collect();
                        debug!(updated_memberlist=?updated_memberlist);
                        if members_sender.send(updated_memberlist).is_err() {
                            // Somehow the cluster has been dropped.
                            error!("Failed to send a member list.");
                            break;
                        }
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        debug!("channel disconnected");
                        break;
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        if task_stop.load(Ordering::Relaxed) {
                            debug!("receive a stop signal");
                            break;
                        }
                    }
                }
            }
        });

        Ok(cluster)
    }

    /// Return watchstream for monitoring change of `members`
    pub fn member_change_watcher(&self) -> WatchStream<Vec<Member>> {
        WatchStream::new(self.members.clone())
    }

    /// Return `members` list
    pub fn members(&self) -> Vec<Member> {
        self.members.borrow().clone()
    }

    /// Specify the address of a running node and join the cluster to which the node belongs.
    pub async fn add_peer_node(&self, peer_addr: SocketAddr) {
        info!(self_addr = ?self.listen_addr, peer_addr = ?peer_addr, "Adding peer node.");
        self.artillery_cluster.add_seed_node(peer_addr);
    }

    /// Leave the cluster.
    pub async fn leave(&self) {
        info!(self_addr = ?self.listen_addr, "Leaving the cluster.");
        self.artillery_cluster.leave_cluster();
        self.stop.store(true, Ordering::Relaxed);
    }

    /// Convenience method for testing that waits for the predicate to hold true for the cluster's
    /// members.
    pub async fn wait_for_members<F>(
        self: &Cluster,
        mut predicate: F,
        timeout_after: Duration,
    ) -> anyhow::Result<()>
    where
        F: FnMut(&Vec<Member>) -> bool,
    {
        timeout(
            timeout_after,
            self.member_change_watcher()
                .skip_while(|members| !predicate(members))
                .next(),
        )
        .await?;
        Ok(())
    }
}

/// Convert the Artillery's member into nucliadb one.
fn convert_member(member: ArtilleryMember, self_listen_addr: SocketAddr) -> Member {
    let listen_addr = if let Some(addr) = member.remote_host() {
        addr
    } else {
        self_listen_addr
    };

    Member {
        node_id: member.node_id(),
        listen_addr,
        node_type: member.node_type(),
        is_self: member.is_current(),
    }
}

/// Output member event as log.
fn log_artillery_event(node_id: String, artillery_member_event: ArtilleryMemberEvent) {
    match artillery_member_event {
        ArtilleryMemberEvent::Joined(artillery_member) => {
            debug!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), "Joined.");
            debug!("{} {} Joined.", node_id, artillery_member.node_id(),);
        }
        ArtilleryMemberEvent::WentUp(artillery_member) => {
            info!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), "Went up.");
            debug!("{} {} Went up.", node_id, artillery_member.node_id(),);
        }
        ArtilleryMemberEvent::SuspectedDown(artillery_member) => {
            warn!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), "Suspected down.");
            debug!("{} {} Suspected down.", node_id, artillery_member.node_id(),);
        }
        ArtilleryMemberEvent::WentDown(artillery_member) => {
            error!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), "Went down.");
            debug!("{} {} Went down.", node_id, artillery_member.node_id(),);
        }
        ArtilleryMemberEvent::Left(artillery_member) => {
            info!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), "Left.");
            debug!("{} {} Left.", node_id, artillery_member.node_id(),);
        }
        ArtilleryMemberEvent::Payload(artillery_member, message) => {
            info!(host_key=?artillery_member.node_id(), remote_host=?artillery_member.remote_host(), message=?message, "Payload.");
            debug!("{} {} Payload.", node_id, artillery_member.node_id(),);
        }
    };
}

pub fn find_available_port() -> anyhow::Result<u16> {
    let socket = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
    let listener = TcpListener::bind(socket)?;
    let port = listener.local_addr()?.port();
    Ok(port)
}

pub fn create_cluster_for_test_with_id(peer_uuid: String) -> anyhow::Result<Cluster> {
    let port = find_available_port()?;
    let peer_addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), port);
    let cluster = Cluster::new(peer_uuid, peer_addr, 'N', 2, 2)?;
    Ok(cluster)
}

/// Creates a local cluster listening on a random port.
pub fn create_cluster_for_test() -> anyhow::Result<Cluster> {
    let peer_uuid = Uuid::new_v4().to_string();
    let cluster = create_cluster_for_test_with_id(peer_uuid)?;
    Ok(cluster)
}

pub fn setup_logging_for_tests() {
    use std::sync::Once;
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        env_logger::builder().format_timestamp(None).init();
    });
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use itertools::Itertools;
    use nucliadb_swim::prelude::{ArtilleryMember, ArtilleryMemberState};
    use tokio::time::sleep;

    use super::*;
    use crate::cluster::{convert_member, Member};

    #[tokio::test]
    async fn test_cluster_convert_member() {
        let node_id = Uuid::new_v4().to_string();
        let remote_host = SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 0);
        {
            let artillery_member = ArtilleryMember::new(
                node_id.clone(),
                remote_host,
                0,
                'N',
                ArtilleryMemberState::Alive,
            );

            let member = convert_member(artillery_member, remote_host);
            let expected_member = Member {
                node_id: node_id.clone(),
                listen_addr: remote_host,
                node_type: 'N',
                is_self: false,
            };
            assert_eq!(member, expected_member);
        }
        {
            let artillery_member = ArtilleryMember::current(node_id.clone(), 'N');
            let member = convert_member(artillery_member, remote_host);
            let expected_member = Member {
                node_id,
                listen_addr: remote_host,
                node_type: 'N',
                is_self: true,
            };
            assert_eq!(member, expected_member);
        }
    }

    #[tokio::test]
    async fn test_cluster_single_node() -> anyhow::Result<()> {
        let cluster = create_cluster_for_test()?;

        let members: Vec<SocketAddr> = cluster
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .collect();
        let expected_members = vec![cluster.listen_addr];
        assert_eq!(members, expected_members);

        cluster.leave().await;
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_multiple_nodes() -> anyhow::Result<()> {
        setup_logging_for_tests();
        let cluster1 = create_cluster_for_test()?;
        let cluster2 = create_cluster_for_test()?;
        let cluster3 = create_cluster_for_test()?;

        cluster2.add_peer_node(cluster1.listen_addr).await;
        cluster3.add_peer_node(cluster1.listen_addr).await;

        let ten_secs = Duration::from_secs(10);

        for cluster in [&cluster1, &cluster2, &cluster3] {
            cluster
                .wait_for_members(|members| members.len() == 3, ten_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        drop(cluster2);
        cluster1
            .wait_for_members(|members| members.len() == 2, ten_secs)
            .await
            .unwrap();

        cluster3.leave().await;
        cluster1
            .wait_for_members(|members| members.len() == 1, ten_secs)
            .await
            .unwrap();
        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_issue_1018() -> anyhow::Result<()> {
        setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id("cluster1".to_string())?;
        let cluster2 = create_cluster_for_test_with_id("cluster2".to_string())?;

        cluster2.add_peer_node(cluster1.listen_addr).await;

        let ten_secs = Duration::from_secs(10);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 2, ten_secs)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .sorted()
            .collect();
        let mut expected_members = vec![cluster1.listen_addr, cluster2.listen_addr];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        cluster2.leave().await;
        drop(cluster2);
        cluster1
            .wait_for_members(|members| members.len() == 1, ten_secs)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let cluster2 = Cluster::new("newid".to_string(), cluster2_listen_addr, 'N', 2)?;
        cluster2.add_peer_node(cluster1.listen_addr).await;

        for _ in 0..4_000 {
            if cluster1.members().len() > 2 {
                panic!("too many members");
            }
            sleep(Duration::from_millis(1)).await;
        }

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster2"));

        Ok(())
    }

    #[tokio::test]
    async fn test_cluster_rejoin_with_different_id_3_nodes_issue_1018() -> anyhow::Result<()> {
        setup_logging_for_tests();
        let cluster1 = create_cluster_for_test_with_id("cluster1".to_string())?;
        let cluster2 = create_cluster_for_test_with_id("cluster2".to_string())?;
        let cluster3 = create_cluster_for_test_with_id("cluster3".to_string())?;

        cluster2.add_peer_node(cluster1.listen_addr).await;
        cluster3.add_peer_node(cluster2.listen_addr).await;

        let wait_period = Duration::from_secs(15);

        for cluster in [&cluster1, &cluster2] {
            cluster
                .wait_for_members(|members| members.len() == 3, wait_period)
                .await
                .unwrap();
        }
        let members: Vec<SocketAddr> = cluster1
            .members()
            .iter()
            .map(|member| member.listen_addr)
            .sorted()
            .collect();
        let mut expected_members = vec![
            cluster1.listen_addr,
            cluster2.listen_addr,
            cluster3.listen_addr,
        ];
        expected_members.sort();
        assert_eq!(members, expected_members);

        let cluster2_listen_addr = cluster2.listen_addr;
        let cluster3_listen_addr = cluster3.listen_addr;
        drop(cluster2);
        drop(cluster3);
        cluster1
            .wait_for_members(|members| members.len() == 1, wait_period)
            .await
            .unwrap();

        sleep(Duration::from_secs(3)).await;

        let cluster2 = Cluster::new("newid".to_string(), cluster2_listen_addr, 'N', 2)?;
        cluster2.add_peer_node(cluster1.listen_addr).await;

        let cluster3 = Cluster::new("newid2".to_string(), cluster3_listen_addr, 'N', 2)?;
        cluster3.add_peer_node(cluster2.listen_addr).await;

        sleep(Duration::from_secs(10)).await;

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster2"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster2"));
        assert!(!cluster3
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster2"));

        assert!(!cluster1
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster3"));
        assert!(!cluster2
            .members()
            .iter()
            .any(|member| (*member).node_id == "cluster3"));

        Ok(())
    }
}
