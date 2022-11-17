use std::net::SocketAddr;
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use log::{debug, error, info};
use nucliadb_cluster::cluster::{Cluster, NodeType};
use rand::Rng;
use structopt::StructOpt;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net;
use tokio::net::TcpStream;
use tokio::signal::unix::{signal, SignalKind};
use tokio::time::{sleep, timeout};
use uuid::Uuid;

#[derive(Debug, StructOpt)]
struct ClusterMgrArgs {
    #[structopt(short, long, env = "LISTEN_PORT")]
    listen_port: String,
    #[structopt(short, long, env = "NODE_TYPE")]
    node_type: NodeType,
    #[structopt(short, long, env = "SEEDS", value_delimiter = ";")]
    seeds: Vec<String>,
    #[structopt(short, long, env = "MONITOR_ADDR")]
    monitor_addr: String,
    #[structopt(short, long, env = "HOSTNAME")]
    pub_ip: String,
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

async fn send_update(update: String, stream: &mut TcpStream) -> anyhow::Result<()> {
    debug!("write_buf");
    if let Err(e) = stream.write_buf(&mut update.as_bytes()).await {
        error!("Error during writing cluster members vector: {e}")
    };
    debug!("Try flush");
    if let Err(e) = stream.flush().await {
        error!("Error during flushing writer: {e}")
    };
    info!("Try read the answer");
    let mut buf = vec![];
    if let Ok(readed) = stream.read_buf(&mut buf).await {
        info!("answer from server: {:#?}", buf);
        if readed != 0 {
            info!("valid answer receieved: {:#?}", buf);
            Ok(())
        } else {
            info!("invalid ack: {:#?}", buf);
            Err(anyhow!("invalid ack"))
        }
    } else {
        info!("invalid ack: {:#?}", buf);
        Err(anyhow!("invalid ack"))
    }
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
    let args = ClusterMgrArgs::from_args();

    let mut termination = signal(SignalKind::terminate())?;

    let host = format!("{}:{}", &args.pub_ip, &args.listen_port);
    let addr = reliable_lookup_host(&host).await?;
    let node_id = Uuid::new_v4();
    let cluster = Cluster::new(node_id.to_string(), addr, args.node_type, args.seeds)
        .await
        .with_context(|| "Can't create cluster instance ")?;

    let mut watcher = cluster.members_change_watcher();
    let mut writer = get_stream(args.monitor_addr.clone())
        .await
        .with_context(|| "Can't create update writer")?;
    loop {
        tokio::select! {
            _ = termination.recv() => {
                cluster.shutdown().await;
                writer.shutdown().await?;
                break
            },
            res = watcher.changed() => {
                debug!("Something changed");
                if let Err(e) = res {
                    error!("update received with error: {e}");
                    continue
                }
                let update = &*watcher.borrow();
                if !update.is_empty() {
                    let ser = match serde_json::to_string(&update) {
                        Ok(s) => s,
                        Err(e) => {
                            error!("Error during vector of members serialization: {e}");
                            continue
                        }
                    };
                    debug!("Serialized update {ser}");
                    while let Ok(false) = watcher.has_changed() {
                        if check_peer(&mut writer).await? {
                            debug!("Correct peer response");
                            if let Ok(()) = send_update(ser.clone(), &mut writer).await {
                                debug!("Update sended");
                                break
                            }
                            debug!("send update failed")
                        } else {
                            error!("Check peer failed before update sending. Sleep 200ms and reconnect");
                            writer.shutdown().await?;
                            writer = get_stream(args.monitor_addr.clone()).await?
                        }
                    }
                };
            }
        };
    }
    Ok(())
}
