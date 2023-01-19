use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use clap::Parser;
use log::{debug, error, info};
use nucliadb_cluster::{node, Node, NodeSnapshot, NodeType};
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{self, TcpStream};
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, timeout};
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

async fn check_peer(stream: &mut TcpStream) -> anyhow::Result<bool> {
    let mut rng = rand::thread_rng();
    let syn = rng.gen::<u32>().to_be_bytes();
    let _ = stream.write(&syn).await?;
    debug!("Sended syn: {:?}", syn);
    let hash = crc32fast::hash(&syn);
    debug!("Calculated {hash}");
    let mut response_buf: [u8; 4] = [0; 4];

    match timeout(Duration::from_secs(1), stream.read(&mut response_buf)).await {
        Ok(Ok(r)) => {
            if r == 4 {
                let response = u32::from_be_bytes(response_buf);
                if response == hash {
                    debug!("[+] Correct response receieved");
                    Ok(true)
                } else {
                    debug!("Incorrect hash received: {response}");
                    Ok(false)
                }
            } else {
                debug!("Incorrect number of bytes readed from socket: {r}");
                Ok(false)
            }
        }
        Ok(Err(e)) => {
            debug!("Error during reading from socket: {e}");
            Ok(false)
        }
        Err(e) => {
            debug!("Don't receive answer during 1 sec: {e}");
            Ok(false)
        }
    }
}

async fn get_stream(monitor_addr: String) -> anyhow::Result<TcpStream> {
    loop {
        match TcpStream::connect(&monitor_addr).await {
            Ok(mut s) => {
                if check_peer(&mut s).await? {
                    break Ok(s);
                }
                debug!("Invalid peer. Sleep 1s and reconnect");
                tokio::time::sleep(Duration::from_secs(1)).await;
                s.shutdown().await?
            }
            Err(e) => {
                error!("Can't connect to monitor socket: {e}. Sleep 200ms and reconnect");
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        }
    }
}

async fn send_update(
    cluster_snapshot: Vec<NodeSnapshot>,
    stream: &mut TcpStream,
    args: &Args,
) -> anyhow::Result<()> {
    if !check_peer(stream).await? {
        error!("Check peer failed before cluster snapshot sending. Try to reconnect");

        stream.shutdown().await?;
        *stream = get_stream(args.monitor_addr.clone()).await?;
    }

    if !cluster_snapshot.is_empty() {
        let serial = serde_json::to_string(&cluster_snapshot)
            .map_err(|e| anyhow!("Cannot serialize cluster cluster snapshot: {e}"))?;

        stream
            .write_buf(&mut serial.as_bytes())
            .await
            .map_err(|e| anyhow!("Error during sending cluster cluster snapshot: {e}"))?;

        stream
            .flush()
            .await
            .map_err(|e| anyhow!("Error during flushing stream: {e}"))?;

        let mut buffer = vec![];

        stream
            .read_buf(&mut buffer)
            .await
            .map_err(Into::into)
            .and_then(|n| {
                if n == 0 {
                    Err(anyhow!("None update answer"))
                } else if buffer.try_into().map(u32::from_be_bytes)
                    != Ok(cluster_snapshot.len() as u32)
                {
                    Err(anyhow!("Received invalid update answer"))
                } else {
                    Ok(())
                }
            })?;
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
    let mut writer = get_stream(arg.monitor_addr.clone())
        .await
        .with_context(|| "Can't create update writer")?;

    loop {
        tokio::select! {
            _ = termination.recv() => {
                node.shutdown().await?;
                writer.shutdown().await?;
                break
            },
            _ = sleep(arg.update_interval) => {
                debug!("Fixed cluster update");

                let live_nodes = node.live_nodes().await;
                let cluster_snapshot = node::cluster_snapshot(live_nodes).await;

                debug!("Cluster snapshot {cluster_snapshot:?}");

                if let Err(e) = send_update(cluster_snapshot, &mut writer, &arg).await {
                    error!("Send cluster cluster_snapshot failed: {e}");
                } else {
                    info!("Update sended")
                }
            },
            Some(live_nodes) = cluster_watcher.next() => {
                debug!("Something changed in cluster");

                let cluster_snapshot = node::cluster_snapshot(live_nodes).await;

                debug!("Cluster snapshot {cluster_snapshot:?}");

                if let Err(e) = send_update(cluster_snapshot, &mut writer, &arg).await {
                    error!("Send cluster cluster_snapshot failed: {e}");
                } else {
                    info!("Update sended")
                }
            }
        };
    }
    Ok(())
}
