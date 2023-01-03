use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::Result;
use nucliadb_ftp::{ReceiveOptions, SendOptions};

#[derive(Parser)]
struct Opt {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Send {
        address: SocketAddr,
        source: PathBuf,
    },
    Listen {
        #[arg(short, long)]
        port: u16,
        destination: PathBuf,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    color_eyre::install()?;

    let opt = Opt::parse();

    match opt.command {
        Command::Send { address, source } => {
            nucliadb_ftp::send_with_options(
                address,
                source,
                SendOptions {
                    append_recursively: true,
                    follow_symlinks: false,
                    preserve_metadata: true,
                },
            )
            .await?
        }
        Command::Listen { port, destination } => {
            nucliadb_ftp::receive_with_options(
                format!("0.0.0.0:{}", port),
                destination,
                ReceiveOptions {
                    preserve_metadata: false,
                },
            )
            .await?
        }
    }

    Ok(())
}
