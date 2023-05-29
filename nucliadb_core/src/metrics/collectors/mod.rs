mod console;
mod noop;
mod prometheus;

pub use console::ConsoleLogMetricsCollector;
pub use noop::NoOpMetricsCollector;
pub use prometheus::PrometheusMetricsCollector;

use crate::metrics::request_time;
use crate::NodeResult;

pub trait MetricsCollector: Send + Sync {
    fn export(&self) -> NodeResult<String>;
    fn record_request_time(
        &self,
        metric: request_time::RequestTimeKey,
        value: request_time::RequestTimeValue,
    );
}
