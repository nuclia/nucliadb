use tracing::{debug, error};
use tracing_subscriber::filter::{FilterFn, Targets};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::Layer;

use crate::config::Configuration;
use crate::result::{ServiceError, ServiceResult};

pub fn init_telemetry() -> ServiceResult<()> {
    let agent_endpoint = Configuration::jaeger_agent_endp();
    debug!("{agent_endpoint}");
    let log_levels = Configuration::log_level();

    let mut layers = Vec::new();

    if Configuration::jaeger_enabled() {
        let tracer = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(agent_endpoint)
            .with_service_name("nucliadb_node")
            .with_auto_split_batch(true)
            .with_max_packet_size(9216) // just for MacOS local tests
            .install_batch(opentelemetry::runtime::Tokio)
            .map_err(|e| ServiceError::GenericErr(Box::new(e)))?;

        let filter = FilterFn::new(|metadata| {
            let target = metadata.target();
            match metadata.module_path() {
                Some(module_path) if module_path.contains("nucliadb_node") => {
                    target.contains("nucliadb_node::writer")
                        || target.contains("nucliadb_node::reader")
                }
                _ => false,
            }
        });
        let jaeger_layer = tracing_opentelemetry::layer()
            .with_tracer(tracer)
            .with_filter(filter)
            .boxed();
        layers.push(jaeger_layer);
    }

    let stdout_layer = tracing_subscriber::fmt::layer()
        .pretty()
        .with_level(true)
        .with_filter(Targets::new().with_targets(log_levels))
        .boxed();

    layers.push(stdout_layer);

    tracing_subscriber::registry()
        .with(layers)
        .try_init()
        .map_err(|e| {
            error!("Try init error: {e}");
            ServiceError::GenericErr(Box::new(e))
        })?;
    Ok(())
}
