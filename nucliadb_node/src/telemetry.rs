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

use std::collections::HashMap;

use nucliadb_core::tracing::{Level, Metadata, Span};
use nucliadb_core::{Context, NodeResult};
use opentelemetry::global;
use opentelemetry::trace::TraceContextExt;
use sentry::ClientInitGuard;
use tracing_core::subscriber::Interest;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use tracing_subscriber::filter::{FilterFn, LevelFilter};
use tracing_subscriber::layer::{Context as LayerContext, Filter, Layer, SubscriberExt};
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Registry;

use crate::settings::Settings;
use crate::utils::ALL_TARGETS;

const TRACE_ID: &str = "trace-id";

pub fn init_telemetry(settings: &Settings) -> NodeResult<Option<ClientInitGuard>> {
    let mut layers = Vec::new();

    let stdout = stdout_layer(settings);
    layers.push(stdout);

    if settings.jaeger_enabled() {
        let jaeger = jaeger_layer(settings)?;
        layers.push(jaeger);
    }

    let sentry_guard;

    if settings.sentry_enabled() {
        sentry_guard = Some(setup_sentry(settings.sentry_env(), settings.sentry_url()));
        let sentry = sentry_layer();
        layers.push(sentry);
    } else {
        eprintln!("Sentry disabled");
        sentry_guard = None;
    }

    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .with_context(|| "trying to init tracing")?;

    Ok(sentry_guard)
}

pub struct LogLevelsFilter {
    prefixes: Vec<(String, Level)>,
    all_target_level: LevelFilter,
    logs_map: HashMap<String, Level>,
}

impl LogLevelsFilter {
    fn new(log_levels: Vec<(String, Level)>) -> Self {
        let mut logs_map = HashMap::new();
        let mut all_target_level = LevelFilter::OFF;
        let mut prefixes = vec![];

        for (target, log_level) in log_levels.iter() {
            if target == ALL_TARGETS {
                all_target_level = LevelFilter::from_level(*log_level);
            } else if target.ends_with('*') {
                prefixes.push((target[..target.len() - 1].to_owned(), *log_level));
            } else {
                logs_map.insert(target.clone(), *log_level);
            }
        }

        LogLevelsFilter {
            prefixes,
            all_target_level,
            logs_map,
        }
    }

    fn is_enabled(&self, metadata: &Metadata<'_>) -> bool {
        let level = metadata.level();

        // match all
        if self.all_target_level != LevelFilter::OFF && self.all_target_level >= *level {
            return true;
        }

        let metadata_target = metadata.target();

        // exact match
        if self.logs_map.get(metadata_target) >= Some(level) {
            return true;
        }

        // prefixes match
        let possible_match = self
            .prefixes
            .iter()
            .find(|(prefix, log_level)| log_level >= level && metadata_target.starts_with(prefix));

        possible_match.is_some()
    }
}

impl<S> Filter<S> for LogLevelsFilter {
    fn enabled(&self, metadata: &Metadata<'_>, _: &LayerContext<'_, S>) -> bool {
        self.is_enabled(metadata)
    }
    fn callsite_enabled(&self, metadata: &'static Metadata<'static>) -> Interest {
        // The result of `self.enabled(metadata, ...)` will always be
        // the same for any given `Metadata`, so we can convert it into
        // an `Interest`:
        if self.is_enabled(metadata) {
            Interest::always()
        } else {
            Interest::never()
        }
    }
}

/// Sets up the stdout output using the log levels (target, level)
/// Log levels can be defined with 3 different flavors to match targets (crate names)
/// - exact match. e.g. `node_reader`
/// - partial match. e.g. `node*` will match all targets starting with `node`
/// - catch-all using `*`
fn stdout_layer(settings: &Settings) -> Box<dyn Layer<Registry> + Send + Sync> {
    let log_levels = settings.log_levels().to_vec();
    let layer = tracing_subscriber::fmt::layer()
        .with_level(true)
        .with_target(true);

    let filter = LogLevelsFilter::new(log_levels);

    if settings.plain_logs() || settings.debug() {
        layer
            .event_format(tracing_subscriber::fmt::format().compact())
            .with_filter(filter)
            .boxed()
    } else {
        layer
            .event_format(tracing_subscriber::fmt::format::Format::default().json())
            .fmt_fields(tracing_subscriber::fmt::format::JsonFields::new())
            .with_filter(filter)
            .boxed()
    }
}

fn jaeger_layer(settings: &Settings) -> NodeResult<Box<dyn Layer<Registry> + Send + Sync>> {
    global::set_text_map_propagator(opentelemetry_zipkin::Propagator::new());

    let agent_endpoint = settings.jaeger_agent_address();
    let tracer = opentelemetry_jaeger::new_pipeline()
        .with_agent_endpoint(agent_endpoint)
        .with_service_name("nucliadb_node")
        .with_auto_split_batch(true)
        .install_batch(opentelemetry::runtime::Tokio)?;

    // To avoid sending too much information to Jaeger, we filter out all events
    // (as they are logged to stdout), spans from external instrumented crates
    // (like tantivy, hyper, tower, mio...) and spans below INFO level (default
    // span level).
    let level_filter = LevelFilter::from_level(Level::INFO);
    let span_filter = FilterFn::new(|metadata| {
        metadata.is_span()
            && metadata
                .file()
                .filter(|file| file.contains("nucliadb"))
                .is_some()
    });

    Ok(tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(level_filter)
        .with_filter(span_filter)
        .boxed())
}

fn setup_sentry(env: &'static str, sentry_url: String) -> ClientInitGuard {
    sentry::init((
        sentry_url,
        sentry::ClientOptions {
            release: sentry::release_name!(),
            environment: Some(env.into()),
            ..Default::default()
        },
    ))
}

fn sentry_layer() -> Box<dyn Layer<Registry> + Send + Sync> {
    sentry_tracing::layer().boxed()
}

pub fn run_with_telemetry<F, R>(current: Span, f: F) -> R
where F: FnOnce() -> R {
    let tid = current.context().span().span_context().trace_id();
    sentry::with_scope(|scope| scope.set_tag(TRACE_ID, tid), || current.in_scope(f))
}

#[cfg(test)]
mod tests {
    use tracing_core::metadata::Kind;
    use tracing_core::{callsite, metadata};

    use super::*;

    pub struct MyCallsite {}

    impl callsite::Callsite for MyCallsite {
        fn set_interest(&self, _: Interest) {
            unimplemented!()
        }
        fn metadata(&self) -> &Metadata {
            unimplemented!()
        }
    }

    static FOO_CALLSITE: MyCallsite = MyCallsite {
            // ...
    };

    #[test]
    fn test_logs_filtering_catchall() {
        // we catch all logs at the INFO level
        let log_levels = vec![("*".to_string(), Level::INFO)];
        let filter = LogLevelsFilter::new(log_levels);

        for (target, level, should_log) in [
            ("some_node", Level::DEBUG, false),
            ("some_node", Level::INFO, true),
            ("unknown", Level::DEBUG, false),
            ("nucliadb_node", Level::INFO, true),
            ("nucliadb_node", Level::DEBUG, false),
        ] {
            let metadata = metadata!(
                name:"metadata",
                target:target,
                level: level,
                fields: &["bar", "baz"],
                callsite: &FOO_CALLSITE,
                kind: Kind::SPAN,
            );
            if should_log {
                assert!(filter.is_enabled(&metadata));
            } else {
                assert!(!filter.is_enabled(&metadata));
            }
        }
    }
    #[test]
    fn test_logs_filtering_partial_and_exact() {
        // defined logs
        let log_levels = vec![
            ("nucliadb_node".to_string(), Level::INFO),
            ("some*".to_string(), Level::DEBUG),
        ];

        let filter = LogLevelsFilter::new(log_levels);

        for (target, level, should_log) in [
            ("some_node", Level::DEBUG, true),
            ("some_node", Level::INFO, true),
            ("unknown", Level::DEBUG, false),
            ("nucliadb_node", Level::INFO, true),
            ("nucliadb_node", Level::DEBUG, false),
        ] {
            let metadata = metadata!(
                name:"metadata",
                target:target,
                level: level,
                fields: &["bar", "baz"],
                callsite: &FOO_CALLSITE,
                kind: Kind::SPAN,
            );
            if should_log {
                assert!(filter.is_enabled(&metadata));
            } else {
                assert!(!filter.is_enabled(&metadata));
            }
        }
    }
}
