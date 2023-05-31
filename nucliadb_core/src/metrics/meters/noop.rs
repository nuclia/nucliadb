use crate::metrics::meters::Meter;
use crate::metrics::metrics::request_time;
use crate::NodeResult;

pub struct NoOpMeter;
impl Meter for NoOpMeter {
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
