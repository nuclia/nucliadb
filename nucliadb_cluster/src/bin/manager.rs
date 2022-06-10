use anyhow::Context;
use nucliadb_cluster::cluster::{Cluster, NucliaDBNodeType};
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::signal::unix::{signal, SignalKind};
use std::net::SocketAddr;
use std::str::FromStr;
use std::time::Duration;
use uuid::Uuid;
use clap::Parser;
use tokio::net::{TcpStream, ToSocketAddrs};
use log::error;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct CliArgs {
    #[clap(short, long)]
    listen_addr: String,

    #[clap(short, long)]
    node_type: String,

    #[clap(short, long)]
    seeds: Vec<String>,

    #[clap(short, long)]
    monitor_addr: Option<String>
}

async fn get_writer(monitor_addr: Option<String>) -> anyhow::Result<Box<dyn AsyncWrite + Unpin>> {
    if let Some(socket_addr) = monitor_addr {
        let monitor_sock = loop {
            match TcpStream::connect(&socket_addr).await {
                Ok(s) => break s,
                Err(e) => {
                    error!("Can't connect to monitor socket: {e}. Sleep 200ms and reconnect");
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    continue
                }
            }
        };
        Ok(Box::new(monitor_sock))
    } else {
        let std_io = tokio::io::stdout();
        Ok(Box::new(std_io))
    }   
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();
    let args = CliArgs::parse();

    let mut termination = signal(SignalKind::terminate())?;

    let addr = SocketAddr::from_str(&args.listen_addr).with_context(|| "Can't create cluster listener socket")?;
    let node_type = NucliaDBNodeType::from_str(&args.node_type).with_context(|| "Can't parse node type")?;
    let node_id = Uuid::new_v4();
    let cluster = Cluster::new(
        node_id.to_string(),
        addr,
        node_type,
        args.seeds
    ).await.with_context(|| "Can't create cluster instance ")?;

    let mut watcher = cluster.members_change_watcher();
    let mut writer =  get_writer(args.monitor_addr).await.with_context(|| "Can't create update writer")?;
    
    loop {
        tokio::select! {
            _ = termination.recv() => {
                cluster.shutdown().await;
                writer.shutdown().await?;
                break
            },
            res = watcher.changed() => {
                if let Err(e) = res {
                    error!("update received with error: {e}");
                    continue
                }
                let update = &*watcher.borrow();
                if update.is_empty() { //skip first message from self node
                    continue
                };
                let ser = match serde_json::to_string(&update){
                    Ok(s) => s, 
                    Err(e) => {
                        error!("Error during vector of members serialization: {e}");
                        continue
                    }
                };
                if let Err(e) = writer.write_all(ser.as_bytes()).await {
                    error!("Error during writing cluster members vector: {e}")
                };
                if let Err(e) = writer.flush().await {
                    error!("Error during flushing writer: {e}")
                }
            }
        };
    }
    Ok(())
}