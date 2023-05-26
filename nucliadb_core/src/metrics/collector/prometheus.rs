use prometheus_client::encoding;
use prometheus_client::registry::Registry;

use crate::metrics::collector::MetricsCollector;
use crate::metrics::request_time;
use crate::NodeResult;

pub struct PrometheusMetricsCollector {
    registry: Registry,
    request_time_metric: request_time::RequestTimeMetric,
}

impl Default for PrometheusMetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsCollector for PrometheusMetricsCollector {
    fn export(&self) -> NodeResult<String> {
        let mut buf = String::new();
        encoding::text::encode(&mut buf, &self.registry)?;
        Ok(buf)
    }

    fn record_request_time(
        &self,
        metric: request_time::RequestTimeKey,
        value: request_time::RequestTimeValue,
    ) {
        self.request_time_metric
            .get_or_create(&metric)
            .observe(value);
    }
}

impl PrometheusMetricsCollector {
    pub fn new() -> PrometheusMetricsCollector {
        let mut registry = Registry::default();

        // This must be done for every metric
        let request_time_metric = request_time::register_request_time(&mut registry);

        PrometheusMetricsCollector {
            registry,
            request_time_metric,
        }
    }
}
