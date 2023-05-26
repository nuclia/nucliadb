use crate::metrics::collector::MetricsCollector;
use crate::metrics::request_time;
use crate::NodeResult;

pub struct NoOpMetricsCollector;
impl MetricsCollector for NoOpMetricsCollector {
    fn export(&self) -> NodeResult<String> {
        Ok(Default::default())
    }
    fn record_request_time(
        &self,
        _: request_time::RequestTimeKey,
        _: request_time::RequestTimeValue,
    ) {
    }
}
