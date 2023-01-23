use once_cell::sync::OnceCell;

use crate::payload::TelemetryEvent;
pub use crate::sender::is_telemetry_enabled;
use crate::sender::{TelemetryLoopHandle, TelemetrySender};

pub fn start_telemetry_loop() -> TelemetryLoopHandle {
    get_telemetry_sender_singleton().start_loop()
}

fn get_telemetry_sender_singleton() -> &'static TelemetrySender {
    static INSTANCE: OnceCell<TelemetrySender> = OnceCell::new();
    INSTANCE.get_or_init(TelemetrySender::default)
}

/// Sends a telemetry event to Nuclia's server via HTTP.
///
/// Telemetry guarantees to send at most 1 request per minute.
/// Each requests can ship at most 10 messages.
///
/// If this methods is called too often, some events will be dropped.
///
/// If the http requests fail, the error will be silent.
///
/// We voluntarily use an enum here to make it easier for reader
/// to audit the type of information that is send home.
pub async fn send_telemetry_event(event: TelemetryEvent) {
    get_telemetry_sender_singleton().send(event).await
}
