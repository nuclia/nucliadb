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

macro_rules! from_env {
    ($builder:ident.$name:ident $(: $parse:ty)?, $env:literal) => {
        if let Ok(value) = std::env::var($env) {
            #[allow(unused_labels)]
            'set: {
                $(let Ok(value) = value.parse::<$parse>() else { break 'set };)*
                $builder.$name(value);
            }
        }
    };
}

pub mod env {
    use std::sync::Arc;
    use std::time::Duration;

    use nucliadb_core::NodeResult;

    use crate::settings::providers::SettingsProvider;
    use crate::settings::InnerSettings;
    use crate::settings::Settings;
    use crate::utils::{parse_log_levels, parse_node_role};

    /// Provide a settings configuration using environment variables. If an env
    /// var is missing, it defaults to `Settings` defaults.
    pub struct EnvSettingsProvider;

    impl SettingsProvider for EnvSettingsProvider {
        fn generate_settings() -> NodeResult<Settings> {
            let inner = envy::from_env::<InnerSettings>()?.into();
            Ok(inner)
            // let mut builder = Settings::builder();

            // if let Ok(debug) = std::env::var("DEBUG") {
            //     if debug == "true" {
            //         builder.with_debug();
            //     }
            // }

            // from_env!(builder.data_path, "DATA_PATH");
            // from_env!(builder.max_shards_per_node: usize, "MAX_NODE_REPLICAS");
            // from_env!(builder.host_key_path, "HOST_KEY_PATH");
            // from_env!(builder.hostname, "HOSTNAME");

            // if let Ok(Ok(true)) = std::env::var("DISABLE_SENTRY").map(|v| v.parse::<bool>()) {
            //     builder.without_sentry();
            // }

            // from_env!(builder.sentry_url, "SENTRY_URL");
            // from_env!(builder.sentry_env, "RUNNING_ENVIRONMENT");

            // if let Ok(levels) = std::env::var("RUST_LOG") {
            //     builder.log_levels(parse_log_levels(&levels));
            // }

            // // support more standard LOG_LEVELS env var
            // if let Ok(levels) = std::env::var("LOG_LEVELS") {
            //     builder.log_levels(parse_log_levels(&levels));
            // }

            // if let Ok(Ok(true)) = std::env::var("JAEGER_ENABLED").map(|v| v.parse::<bool>()) {
            //     builder.with_jaeger_enabled();
            // }

            // from_env!(builder.jaeger_agent_host, "JAEGER_AGENT_HOST");
            // from_env!(builder.jaeger_agent_port: u16, "JAEGER_AGENT_PORT");
            // from_env!(builder.reader_listen_address, "READER_LISTEN_ADDRESS");
            // from_env!(builder.writer_listen_address, "WRITER_LISTEN_ADDRESS");
            // from_env!(builder.metrics_port: u16, "METRICS_PORT");

            // if let Ok(node_role) = std::env::var("NODE_ROLE") {
            //     builder.node_role(parse_node_role(node_role.as_str()));
            // }

            // from_env!(builder.primary_address, "PRIMARY_ADDRESS");

            // if let Ok(Ok(replication_delay_seconds)) =
            //     std::env::var("REPLICATION_DELAY_SECONDS").map(|v| v.parse::<u64>())
            // {
            //     builder.replication_delay(Duration::from_secs(replication_delay_seconds));
            // }

            // from_env!(builder.replication_max_concurrency: u64, "REPLICATION_MAX_CONCURRENCY");

            // if let Ok(Ok(replication_healthy_delay)) =
            //     std::env::var("REPLICATION_HEALTHY_DELAY").map(|v| v.parse::<u64>())
            // {
            //     builder.replication_healthy_delay(Duration::from_secs(replication_healthy_delay));
            // }

            // from_env!(builder.merge_scheduler_max_nodes_in_merge: usize, "SCHEDULER_MAX_NODES_IN_MERGE");
            // from_env!(builder.merge_scheduler_segments_before_merge: usize, "SCHEDULER_SEGMENTS_BEFORE_MERGE");
            // from_env!(builder.merge_on_commit_max_nodes_in_merge: usize, "MAX_NODES_IN_MERGE");
            // from_env!(builder.merge_on_commit_segments_before_merge: usize, "SEGMENTS_BEFORE_MERGE");

            // builder.build()
        }
    }

    #[cfg(test)]
    mod tests {
        use serial_test::serial;
        use tracing::Level;

        /// Tests playing around with environment variables must run
        /// sequentially to avoid conflicts between them
        use super::*;

        #[test]
        #[serial]
        fn test_default_env_settings() {
            // Safe current state of DATA_PATH
            let data_path_copy = std::env::var("DATA_PATH");
            // Remove DATA_PATH for the test
            std::env::remove_var("DATA_PATH");

            let settings = EnvSettingsProvider::generate_settings().unwrap();

            if let Ok(value) = data_path_copy {
                // The state needs to be restored
                std::env::set_var("DATA_PATH", value);
            }

            assert_eq!(settings.shards_path().to_str().unwrap(), "data/shards")
        }

        #[test]
        #[serial]
        fn test_env_settings_data_path() {
            // Safe current state of DATA_PATH
            let data_path_copy = std::env::var("DATA_PATH");
            // set DATA_PATH for the test
            std::env::set_var("DATA_PATH", "mydata");

            let settings = EnvSettingsProvider::generate_settings().unwrap();

            match data_path_copy {
                Ok(value) => std::env::set_var("DATA_PATH", value),
                Err(_) => std::env::remove_var("DATA_PATH"),
            }

            assert_eq!(settings.shards_path().to_str().unwrap(), "mydata/shards");
        }

        #[test]
        #[serial]
        fn test_disable_sentry() {
            // Safe current state of DISABLE_SENTRY
            let disable_sentry_copy = std::env::var("DISABLE_SENTRY");
            // set DISABLE_SENTRY for the test
            std::env::set_var("LOG_LEVELS", "nucliadb=INFO");
            std::env::set_var("REPLICATION_DELAY", "123");

            let settings: InnerSettings = envy::from_env().unwrap();
            match disable_sentry_copy {
                Ok(value) => std::env::set_var("DISABLE_SENTRY", value),
                Err(_) => std::env::remove_var("DISABLE_SENTRY"),
            }

            assert_eq!(settings.replication_delay, Duration::from_secs(123));
            assert_eq!(settings.log_levels, &[("nucliadb".into(), Level::INFO)]);
        }
    }
}
