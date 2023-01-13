use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Result;
use nucliadb_ftp::{Listener, Publisher};

/// File/directory tranfer over network.
#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Opt {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Publish the given path (a file or a whole directory) to the given IP.
    Publish {
        /// The file/directory to publish.
        path: PathBuf,
        /// The IP of the target machine.
        #[arg(short, long)]
        ip: SocketAddr,
    },
    /// Listen incoming files/directories by storing them to the given path.
    Listen {
        /// The location where save incoming files/directories.
        path: PathBuf,
        /// The listen port.
        #[arg(short, long)]
        port: u16,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let opt = Opt::parse();

    match opt.command {
        Command::Publish { path, ip } => Publisher::default().append(path).send_to(ip).await?,
        Command::Listen { path, port } => {
            Listener::default().save_at(path).listen_once(port).await?
        }
    }

    Ok(())
}
