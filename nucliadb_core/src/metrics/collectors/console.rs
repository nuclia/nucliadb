use std::fmt::Debug;

use crate::metrics::collectors::MetricsCollector;
use crate::metrics::request_time;
use crate::{tracing, NodeResult};

pub struct ConsoleLogMetricsCollector;

impl ConsoleLogMetricsCollector {
    fn record<Metric: Debug, Value: Debug>(&self, metric: Metric, value: Value) {
        tracing::debug!("{metric:?} : {value:?}")
    }
}

impl MetricsCollector for ConsoleLogMetricsCollector {
    fn export(&self) -> NodeResult<String> {
        Ok(Default::default())
    }

    fn record_request_time(
        &self,
        metric: request_time::RequestTimeKey,
        value: request_time::RequestTimeValue,
    ) {
        self.record(metric, value)
    }
}
