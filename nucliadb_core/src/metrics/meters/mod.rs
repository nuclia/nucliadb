mod console;
mod noop;
mod prometheus;

pub use console::ConsoleMeter;
pub use noop::NoOpMeter;
pub use prometheus::PrometheusMeter;

use crate::metrics::metrics::request_time;
use crate::metrics::task_monitor::{Monitor, TaskId};
use crate::NodeResult;

pub trait Meter: Send + Sync {
    fn record_request_time(
        &self,
        metric: request_time::RequestTimeKey,
        value: request_time::RequestTimeValue,
    );

    fn export(&self) -> NodeResult<String>;

    fn task_monitor(&self, _task_id: TaskId) -> Option<Monitor> {
        None
    }
}
