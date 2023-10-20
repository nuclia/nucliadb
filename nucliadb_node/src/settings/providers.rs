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

pub use env::EnvSettingsProvider;
use nucliadb_core::NodeResult;

use crate::settings::Settings;

pub trait SettingsProvider {
    fn generate_settings() -> NodeResult<Settings>;
}

pub mod env {
    use nucliadb_core::NodeResult;

    use crate::settings::providers::SettingsProvider;
    use crate::settings::Settings;
    use crate::utils::{parse_log_levels, parse_node_role};

    /// Provide a settings configuration using environment variables. If an env
    /// var is missing, it defaults to `Settings` defaults.
    pub struct EnvSettingsProvider;

    impl SettingsProvider for EnvSettingsProvider {
        fn generate_settings() -> NodeResult<Settings> {
            let mut builder = Settings::builder();

            if let Ok(data_path) = std::env::var("DATA_PATH") {
                builder.data_path(data_path);
            }

            if let Ok(Ok(false)) = std::env::var("LAZY_LOADING").map(|v| v.parse()) {
                builder.without_lazy_loading();
            }

            if let Ok(Ok(max_shards)) =
                std::env::var("MAX_NODE_REPLICAS").map(|v| v.parse::<usize>())
            {
                builder.max_shards_per_node(max_shards);
            }

            if let Ok(path) = std::env::var("HOST_KEY_PATH") {
                builder.host_key_path(path);
            }

            if let Ok(hostname) = std::env::var("HOSTNAME") {
                builder.hostname(hostname);
            }

            if let Ok(Ok(true)) = std::env::var("DISABLE_SENTRY").map(|v| v.parse::<bool>()) {
                builder.sentry_enabled = Some(false);
            }

            if let Ok(url) = std::env::var("SENTRY_URL") {
                builder.sentry_url(url);
            }

            if let Ok(env) = std::env::var("RUNNING_ENVIRONMENT") {
                builder.sentry_env(env);
            }

            if let Ok(levels) = std::env::var("RUST_LOG") {
                builder.log_levels(parse_log_levels(&levels));
            }

            if let Ok(Ok(true)) = std::env::var("JAEGER_ENABLED").map(|v| v.parse::<bool>()) {
                builder.with_jaeger_enabled();
            }

            if let Ok(host) = std::env::var("JAEGER_AGENT_HOST") {
                builder.jaeger_agent_host(host);
            }

            if let Ok(Ok(port)) = std::env::var("JAEGER_AGENT_PORT").map(|v| v.parse::<u16>()) {
                builder.jaeger_agent_port(port);
            }

            if let Ok(addr) = std::env::var("READER_LISTEN_ADDRESS") {
                builder.reader_listen_address(addr);
            }

            if let Ok(addr) = std::env::var("WRITER_LISTEN_ADDRESS") {
                builder.writer_listen_address(addr);
            }

            if let Ok(Ok(port)) = std::env::var("METRICS_PORT").map(|v| v.parse::<u16>()) {
                builder.metrics_port(port);
            }

            if let Ok(node_role) = std::env::var("NODE_ROLE") {
                builder.node_role(parse_node_role(node_role.as_str()));
            }

            if let Ok(primary_address) = std::env::var("PRIMARY_ADDRESS") {
                builder.primary_address(primary_address);
            }

            if let Ok(Ok(replication_delay_seconds)) =
                std::env::var("REPLICATION_DELAY_SECONDS").map(|v| v.parse::<u64>())
            {
                builder.replication_delay_seconds(replication_delay_seconds);
            }

            let settings = builder.build()?;
            Ok(settings)
        }
    }

    #[cfg(test)]
    mod tests {
        use serial_test::serial;

        /// Tests playing around with environment variables must run
        /// sequentially to avoid conflicts between them
        use super::*;

        #[test]
        #[serial]
        fn test_default_env_settings() {
            let settings = EnvSettingsProvider::generate_settings().unwrap();
            assert_eq!(settings.shards_path().to_str().unwrap(), "data/shards")
        }

        #[test]
        #[serial]
        fn test_env_settings_data_path() {
            std::env::set_var("DATA_PATH", "mydata");
            let settings = EnvSettingsProvider::generate_settings().unwrap();
            assert_eq!(settings.shards_path().to_str().unwrap(), "mydata/shards");
            std::env::remove_var("DATA_PATH");
        }

        #[test]
        #[serial]
        fn test_disable_sentry() {
            std::env::set_var("DISABLE_SENTRY", "true");
            let settings = EnvSettingsProvider::generate_settings().unwrap();
            assert!(!settings.sentry_enabled);
            std::env::remove_var("DISABLE_SENTRY");
        }
    }
}
