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

use std::net::SocketAddr;
use std::path::PathBuf;

use clap::{Parser, Subcommand};
use eyre::{eyre, Result};
use nucliadb_ftp::{Listener, Publisher, RetryPolicy};
use tracing_subscriber::EnvFilter;

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
        #[arg(short, long)]
        retry_on_failure: bool,
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

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .try_init()
        .map_err(|e| eyre!(e))?;

    let opt = Opt::parse();

    match opt.command {
        Command::Publish {
            path,
            ip,
            retry_on_failure,
        } => {
            Publisher::default()
                .retry_on_failure(if retry_on_failure {
                    RetryPolicy::Always
                } else {
                    RetryPolicy::Never
                })
                .append(path)?
                .send_to(ip)
                .await?
        }
        Command::Listen { path, port } => {
            Listener::default().save_at(path).listen_once(port).await?
        }
    }

    Ok(())
}
