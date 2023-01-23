use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use crate::payload::{TelemetryEvent, TelemetryPayload};
use crate::sender::is_telemetry_enabled;
use crate::sink::BlockingHttpClient;

struct SyncTelemetryLoop {
    client: BlockingHttpClient,
    rtxn: Receiver<TelemetryEvent>,
}

impl SyncTelemetryLoop {
    pub fn new(rtxn: Receiver<TelemetryEvent>) -> Option<SyncTelemetryLoop> {
        let Some(client) = BlockingHttpClient::try_new() else { return None };
        Some(SyncTelemetryLoop { rtxn, client })
    }
    pub fn run(self) {
        loop {
            match self.rtxn.recv() {
                Err(err) => tracing::info!("channel error {}", err),
                Ok(event) => self
                    .client
                    .blocking_send(TelemetryPayload::from_single_event(event)),
            }
        }
    }
}

lazy_static::lazy_static! {
    static ref SYNC_TELEMETRY: Option<Mutex<Sender<TelemetryEvent>>> = {
        if !is_telemetry_enabled() { return None; }
        let (stxn, rtxn) = channel();
        let Some(telemtry_loop) = SyncTelemetryLoop::new(rtxn) else { return None };
        std::thread::spawn(move || telemtry_loop.run());
        Some(Mutex::new(stxn))
    };
}

pub fn sync_send_telemetry_event(event: TelemetryEvent) {
    if let Some(sender) = SYNC_TELEMETRY.as_ref() {
        // We swallow de error in case of failure
        let sender = sender.lock().unwrap_or_else(|er| er.into_inner());
        let _ = sender.send(event);
    }
}
