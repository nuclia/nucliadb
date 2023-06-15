use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::Parser;
use log::{debug, error, info};
use nucliadb_cluster::{node, Node, NodeSnapshot, NodeType};
use reqwest::Client;
use tokio::net::{self};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::sleep;
use tokio_stream::StreamExt;

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Args {
    #[arg(short, long, env = "LISTEN_PORT")]
    listen_port: String,
    #[arg(short, long, env = "NODE_TYPE")]
    node_type: NodeType,
    #[arg(short, long, env = "SEEDS", value_delimiter = ';')]
    seeds: Vec<String>,
    #[arg(short, long, env = "MONITOR_ADDR")]
    monitor_addr: String,
    #[arg(short, long, env = "HOSTNAME")]
    pub_ip: String,
    #[arg(
        short,
        long,
        env = "UPDATE_INTERVAL",
        default_value = "30s",
        value_parser(parse_duration::parse)
    )]
    update_interval: Duration,
}

async fn send_update(
    cluster_snapshot: Vec<NodeSnapshot>,
    client: &mut Client,
    args: &Args,
) -> anyhow::Result<()> {
    if !cluster_snapshot.is_empty() {
        let url = format!("http://{}/members", &args.monitor_addr);

        let payload = serde_json::to_string(&cluster_snapshot)
            .map_err(|e| anyhow::anyhow!("Cannot serialize cluster cluster snapshot: {e}"))?;

        let resp = client.patch(&url).body(payload).send().await?;

        if !resp.status().is_success() {
            let resp_status = resp.status().as_u16();
            let resp_text = resp.text().await?;
            bail!(format!(
                "Error sending cluster snapshot to monitor. Response HTTP{}: {}",
                &resp_status, &resp_text,
            ))
        }
    }
    Ok(())
}

pub async fn reliable_lookup_host(host: &str) -> anyhow::Result<SocketAddr> {
    let mut tries = 5;
    while tries != 0 {
        if let Ok(mut addr_iter) = net::lookup_host(host).await {
            if let Some(addr) = addr_iter.next() {
                return Ok(addr);
            }
        }
        tries -= 1;
        sleep(Duration::from_secs(1)).await;
    }
    bail!("Can't lookup public ip")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let arg = Args::parse();

    let mut termination = signal(SignalKind::terminate())?;

    let host = format!("{}:{}", &arg.pub_ip, &arg.listen_port);
    let addr = reliable_lookup_host(&host).await?;

    let node = Node::builder()
        .register_as(arg.node_type)
        .on_local_network(addr)
        .with_seed_nodes(arg.seeds.clone())
        .build()
        .with_context(|| "Can't create node instance")?;

    let node = node.start().await?;

    let mut cluster_watcher = node.cluster_watcher().await;
    let mut client = Client::new();
    loop {
        tokio::select! {
            _ = termination.recv() => {
                node.shutdown().await?;
                break
            },
            _ = sleep(arg.update_interval) => {
                debug!("Fixed cluster update");

                let live_nodes = node.live_nodes().await;
                let cluster_snapshot = node::cluster_snapshot(live_nodes).await;

                debug!("Cluster snapshot {cluster_snapshot:?}");

                if let Err(e) = send_update(cluster_snapshot, &mut client, &arg).await {
                    error!("Send cluster cluster_snapshot failed: {e}");
                } else {
                    debug!("Update sent")
                }
            },
            Some(live_nodes) = cluster_watcher.next() => {
                info!("Something changed in the cluster");

                let cluster_snapshot = node::cluster_snapshot(live_nodes).await;

                info!("Cluster snapshot {cluster_snapshot:?}");

                if let Err(e) = send_update(cluster_snapshot, &mut client, &arg).await {
                    error!("Send cluster cluster_snapshot failed: {e}");
                } else {
                    info!("Update sent")
                }
            }
        };
    }
    Ok(())
}
