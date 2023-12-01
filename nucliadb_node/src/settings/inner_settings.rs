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

//! Global settings and providers.
//!
//! This module exports a `Settings` struct thought as a global context for the
//! application. Using diferent providers, one can obtain a `Settings` objects
//! using values from different places.
//!
//! As an example, a `EnvSettingsProvider` collects it's values from environment
//! variables.
//!
//! The trait `SettingsProvider` makes it easy to extend this module with more
//! providers (to parse from CLI for example).

use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::path::PathBuf;

use derive_builder::Builder;
use nucliadb_core::tracing::{error, Level};
use nucliadb_core::NodeResult;

use crate::disk_structure::{METADATA_FILE, SHARDS_DIR};
use crate::replication::NodeRole;
use crate::utils::{parse_log_levels, parse_node_role, reliable_lookup_host};

pub const SENTRY_PROD: &str = "prod";
pub const SENTRY_DEV: &str = "stage";

#[derive(Builder, Debug)]
#[builder(pattern = "mutable", setter(strip_option, into))]
#[builder(build_fn(name = "inner_build"))]
pub struct InnerSettings {
    // Debug
    #[builder(default = "false", setter(custom))]
    pub debug: bool,

    // Data storage and access
    #[builder(default = "\"data\".into()", setter(custom))]
    pub data_path: PathBuf,
    #[builder(private, default = "PathBuf::from(\"data\").join(METADATA_FILE)")]
    pub metadata_path: PathBuf,
    #[builder(private, default = "PathBuf::from(\"data\").join(SHARDS_DIR)")]
    pub shards_path: PathBuf,
    #[builder(default = "true", setter(custom))]
    pub lazy_loading: bool,
    #[builder(default = "800")]
    pub max_shards_per_node: usize,

    // Index node self data
    #[builder(default = "\"host_key\".into()", setter(name = "host_key_path"))]
    pub node_id_path: PathBuf,
    #[builder(default = "reliable_lookup_host(\"localhost\").ip()", setter(custom))]
    pub public_ip: IpAddr,

    // Errors
    #[builder(default = "true", setter(custom))]
    pub sentry_enabled: bool,
    #[builder(default = "String::new()")]
    pub sentry_url: String,
    #[builder(default = "SENTRY_DEV", setter(custom))]
    pub sentry_env: &'static str,

    // Logs and traces
    #[builder(default = "parse_log_levels(\"nucliadb_*=INFO\")")]
    pub log_levels: Vec<(String, Level)>,
    #[rustfmt::skip]
    #[builder(
        default = "parse_log_levels(\"nucliadb_node=INFO,nucliadb_core=INFO\")",
        setter(skip)
    )]
    pub span_levels: Vec<(String, Level)>,

    #[builder(default = "false", setter(custom))]
    pub plain_logs: bool,

    // Telemetry
    #[builder(default = "false", setter(custom))]
    pub jaeger_enabled: bool,
    #[builder(default = "\"localhost\".into()")]
    pub jaeger_agent_host: String,
    #[builder(default = "6831")]
    pub jaeger_agent_port: u16,

    #[builder(default = "reliable_lookup_host(\"localhost:40102\")", setter(custom))]
    pub reader_listen_address: SocketAddr,
    #[builder(default = "reliable_lookup_host(\"localhost:40101\")", setter(custom))]
    pub writer_listen_address: SocketAddr,

    #[builder(default = "3030")]
    pub metrics_port: u16,

    // replications settings
    #[builder(default = "\"http://localhost:10000\".into()")]
    pub primary_address: String,
    #[builder(default = "parse_node_role(\"primary\")")]
    pub node_role: NodeRole,
    #[builder(default = "3")]
    pub replication_delay_seconds: u64,
    #[builder(default = "3")]
    pub replication_max_concurrency: u64,

    // number of seconds since last replication for
    // node to be considered healthy
    #[builder(default = "30")]
    pub replication_healthy_delay: u64,

    // max number of replicas per node
    #[builder(default = "800")]
    pub max_node_replicas: u64,

    // number of threads to use for rayon
    #[builder(default = "10")]
    pub num_global_rayon_threads: usize,
}
impl InnerSettings {
    pub fn builder() -> InnerSettingsBuilder {
        InnerSettingsBuilder::default()
    }
}

impl InnerSettingsBuilder {
    pub fn build(&self) -> NodeResult<super::Settings> {
        let inner = self.inner_build()?;
        Ok(inner.into())
    }

    pub fn with_debug(&mut self) -> &mut Self {
        self.debug = Some(true);
        self
    }

    pub fn data_path(&mut self, data_path: impl Into<PathBuf>) -> &mut Self {
        let data_path = data_path.into();
        self.metadata_path = Some(data_path.join(METADATA_FILE));
        self.shards_path = Some(data_path.join(SHARDS_DIR));
        self.data_path = Some(data_path);
        self
    }

    pub fn without_lazy_loading(&mut self) -> &mut Self {
        self.lazy_loading = Some(false);
        self
    }

    pub fn without_sentry(&mut self) -> &mut Self {
        self.sentry_enabled = Some(false);
        self
    }

    pub fn hostname(&mut self, hostname: impl Into<String>) -> &mut Self {
        let hostname = hostname.into();
        self.public_ip = Some(reliable_lookup_host(&hostname).ip());
        self
    }

    pub fn sentry_env(&mut self, sentry_env: impl Into<String>) -> &mut Self {
        let sentry_env = sentry_env.into();
        if sentry_env == "prod" {
            self.sentry_env = Some(SENTRY_PROD);
        } else if sentry_env == "stage" {
            self.sentry_env = Some(SENTRY_DEV);
        } else {
            error!(
                "Invalid sentry environment: {sentry_env}. Keeping default one: {:?}",
                self.sentry_env
            );
        }
        self
    }

    pub fn with_jaeger_enabled(&mut self) -> &mut Self {
        self.jaeger_enabled = Some(true);
        self
    }

    pub fn reader_listen_address(&mut self, addr: impl Into<String>) -> &mut Self {
        let addr = addr.into();
        self.reader_listen_address = Some(
            addr.to_socket_addrs()
                .unwrap_or_else(|_| panic!("Invalid reader listen address: {}", addr))
                .next()
                .expect("Error parsing socket reader listen address"),
        );
        self
    }

    pub fn writer_listen_address(&mut self, addr: impl Into<String>) -> &mut Self {
        let addr = addr.into();
        self.writer_listen_address = Some(
            addr.to_socket_addrs()
                .unwrap_or_else(|_| panic!("Invalid writer listen address: {}", addr))
                .next()
                .expect("Error parsing socket writer listen address"),
        );
        self
    }
}
