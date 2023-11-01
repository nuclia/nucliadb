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

use std::mem;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::mpsc::{Receiver, Sender};
use tokio::sync::{oneshot, Mutex, RwLock};
use tokio::task::JoinHandle;
use tokio::time::Interval;
use tracing::info;

use crate::analytics::payload::{
    AnalyticsEvent, AnalyticsPayload, ClientInformation, EventWithTimestamp,
};
use crate::analytics::sink::{HttpClient, Sink};

/// At most 1 Request per minutes.
const ANALYTICS_PUSH_COOLDOWN: Duration = Duration::from_secs(60);

/// Upon termination of the program, we send one last analytics request with pending events.
/// This duration is the amount of time we wait for at most to send that last analytics request.
const LAST_REQUEST_TIMEOUT: Duration = Duration::from_secs(1);

const MAX_NUM_EVENTS_IN_QUEUE: usize = 10;

#[cfg(test)]
struct ClockButton(Sender<()>);

#[cfg(test)]
impl ClockButton {
    async fn tick(&self) {
        let _ = self.0.send(()).await;
    }
}

enum Clock {
    Periodical(Mutex<Interval>),
    #[cfg(test)]
    Manual(Mutex<Receiver<()>>),
}

impl Clock {
    pub fn periodical(period: Duration) -> Clock {
        let interval = tokio::time::interval(period);
        Clock::Periodical(Mutex::new(interval))
    }

    #[cfg(test)]
    pub async fn manual() -> (ClockButton, Clock) {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        let _ = tx.send(()).await;
        let button = ClockButton(tx);
        (button, Clock::Manual(Mutex::new(rx)))
    }

    async fn tick(&self) {
        match self {
            Clock::Periodical(interval) => {
                interval.lock().await.tick().await;
            }
            #[cfg(test)]
            Clock::Manual(channel) => {
                channel.lock().await.recv().await;
            }
        }
    }
}

#[derive(Default)]
struct EventsState {
    events: Vec<EventWithTimestamp>,
    num_dropped_events: usize,
}

impl EventsState {
    fn drain_events(&mut self) -> EventsState {
        mem::replace(
            self,
            EventsState {
                events: Vec::new(),
                num_dropped_events: 0,
            },
        )
    }

    /// Adds an event.
    /// If the queue is already saturated, (ie. it has reached the len `MAX_NUM_EVENTS_IN_QUEUE`)
    // Returns true iff it was the first event in the queue.
    fn push_event(&mut self, event: AnalyticsEvent) -> bool {
        if self.events.len() >= MAX_NUM_EVENTS_IN_QUEUE {
            self.num_dropped_events += 1;
            return false;
        }
        let events_was_empty = self.events.is_empty();
        self.events.push(EventWithTimestamp::from(event));
        events_was_empty
    }
}

struct Events {
    state: RwLock<EventsState>,
    items_available_tx: Sender<()>,
    items_available_rx: RwLock<Receiver<()>>,
}

impl Default for Events {
    fn default() -> Self {
        let (items_available_tx, items_available_rx) = tokio::sync::mpsc::channel(1);
        Events {
            state: RwLock::new(EventsState::default()),
            items_available_tx,
            items_available_rx: RwLock::new(items_available_rx),
        }
    }
}

impl Events {
    /// Wait for events to be available (if there are pending events, then do not wait)
    /// and then send them to the PushAPI server.
    async fn drain_events(&self) -> EventsState {
        self.items_available_rx.write().await.recv().await;
        self.state.write().await.drain_events()
    }

    async fn push_event(&self, event: AnalyticsEvent) {
        let is_first_event = self.state.write().await.push_event(event);
        if is_first_event {
            let _ = self.items_available_tx.send(()).await;
        }
    }
}

pub(crate) struct Inner {
    sink: Option<Box<dyn Sink>>,
    client_information: ClientInformation,
    /// This channel is just used to signal there are new items available.
    events: Events,
    clock: Clock,
    is_started: AtomicBool,
}

impl Inner {
    pub fn is_disabled(&self) -> bool {
        self.sink.is_none()
    }

    async fn create_analytics_payload(&self) -> AnalyticsPayload {
        let events_state = self.events.drain_events().await;
        AnalyticsPayload {
            client_information: self.client_information.clone(),
            events: events_state.events,
            num_dropped_events: events_state.num_dropped_events,
        }
    }

    /// Wait for events to be available (if there are pending events, then do not wait)
    /// and then send them to the PushAPI server.
    ///
    /// If the requests fails, it fails silently.
    async fn send_pending_events(&self) {
        if let Some(sink) = self.sink.as_ref() {
            let payload = self.create_analytics_payload().await;
            sink.send_payload(payload).await;
        }
    }

    async fn send(&self, event: AnalyticsEvent) {
        if self.is_disabled() {
            return;
        }
        self.events.push_event(event).await;
    }
}

pub struct AnalyticsSender {
    pub(crate) inner: Arc<Inner>,
}

pub enum AnalyticsLoopHandle {
    NoLoop,
    WithLoop {
        join_handle: JoinHandle<()>,
        terminate_command_tx: oneshot::Sender<()>,
    },
}

impl AnalyticsLoopHandle {
    /// Terminate analytics will exit the analytics loop
    /// and possibly send the last request, possibly ignoring the
    /// analytics cooldown.
    pub async fn terminate_analytics(self) {
        if let Self::WithLoop {
            join_handle,
            terminate_command_tx,
        } = self
        {
            let _ = terminate_command_tx.send(());
            let _ = tokio::time::timeout(LAST_REQUEST_TIMEOUT, join_handle).await;
        }
    }
}

impl AnalyticsSender {
    fn new<S: Sink>(sink_opt: Option<S>, clock: Clock) -> AnalyticsSender {
        let sink_opt: Option<Box<dyn Sink>> = if let Some(sink) = sink_opt {
            Some(Box::new(sink))
        } else {
            None
        };
        AnalyticsSender {
            inner: Arc::new(Inner {
                sink: sink_opt,
                client_information: ClientInformation::default(),
                events: Events::default(),
                clock,
                is_started: AtomicBool::new(false),
            }),
        }
    }

    pub fn start_loop(&self) -> AnalyticsLoopHandle {
        let (terminate_command_tx, mut terminate_command_rx) = oneshot::channel();
        if self.inner.is_disabled() {
            return AnalyticsLoopHandle::NoLoop;
        }

        assert!(
            self.inner
                .is_started
                .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok(),
            "The analytics loop is already started."
        );

        let inner = self.inner.clone();
        let join_handle = tokio::task::spawn(async move {
            // This channel is used to send the command to terminate analytics.
            loop {
                let quit_loop = tokio::select! {
                    _ = (&mut terminate_command_rx) => { true }
                    _ = inner.clock.tick() => { false }
                };
                inner.send_pending_events().await;
                if quit_loop {
                    break;
                }
            }
        });
        AnalyticsLoopHandle::WithLoop {
            join_handle,
            terminate_command_tx,
        }
    }

    pub async fn send(&self, event: AnalyticsEvent) {
        self.inner.send(event).await;
    }
}

/// Check to see if analytics is enabled.
pub fn is_analytics_enabled() -> bool {
    std::env::var_os(crate::analytics::DISABLE_ANALYTICS_ENV_KEY).is_none()
}

fn create_http_client() -> Option<HttpClient> {
    // TODO add analytics URL.
    let client_opt = if is_analytics_enabled() {
        HttpClient::try_new()
    } else {
        None
    };
    if let Some(client) = client_opt.as_ref() {
        info!("analytics to {} is enabled.", client.endpoint());
    } else {
        info!("analytics to nucliadb is disabled.");
    }
    client_opt
}

impl Default for AnalyticsSender {
    fn default() -> Self {
        let http_client = create_http_client();
        AnalyticsSender::new(http_client, Clock::periodical(ANALYTICS_PUSH_COOLDOWN))
    }
}

#[cfg(test)]
mod tests {

    use std::env;

    use super::*;

    #[ignore]
    #[tokio::test]
    async fn test_enabling_and_disabling_analytics() {
        // We group the two in a single test to ensure it happens on the same thread.
        env::set_var(crate::analytics::DISABLE_ANALYTICS_ENV_KEY, "");
        assert_eq!(AnalyticsSender::default().inner.is_disabled(), true);
        env::remove_var(crate::analytics::DISABLE_ANALYTICS_ENV_KEY);
        assert_eq!(AnalyticsSender::default().inner.is_disabled(), false);
    }

    #[tokio::test]
    async fn test_analytics_no_wait_for_first_event() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual().await;
        let analytics_sender = AnalyticsSender::new(Some(tx), clock);
        let loop_handler = analytics_sender.start_loop();
        analytics_sender.send(AnalyticsEvent::Create).await;
        let payload_opt = rx.recv().await;
        assert!(payload_opt.is_some());
        let payload = payload_opt.unwrap();
        assert_eq!(payload.events.len(), 1);
        loop_handler.terminate_analytics().await;
    }

    #[tokio::test]
    async fn test_analytics_two_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let analytics_sender = AnalyticsSender::new(Some(tx), clock);
        let loop_handler = analytics_sender.start_loop();
        analytics_sender.send(AnalyticsEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        clock_btn.tick().await;
        analytics_sender.send(AnalyticsEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        loop_handler.terminate_analytics().await;
    }

    #[tokio::test]
    async fn test_analytics_cooldown_observed() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (clock_btn, clock) = Clock::manual().await;
        let analytics_sender = AnalyticsSender::new(Some(tx), clock);
        let loop_handler = analytics_sender.start_loop();
        analytics_sender.send(AnalyticsEvent::Create).await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 1);
        }
        tokio::task::yield_now().await;
        analytics_sender.send(AnalyticsEvent::Create).await;

        let timeout_res = tokio::time::timeout(Duration::from_millis(1), rx.recv()).await;
        assert!(timeout_res.is_err());

        analytics_sender.send(AnalyticsEvent::Create).await;
        clock_btn.tick().await;
        {
            let payload = rx.recv().await.unwrap();
            assert_eq!(payload.events.len(), 2);
        }
        loop_handler.terminate_analytics().await;
    }

    #[tokio::test]
    async fn test_terminate_analytics_sends_pending_events() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        let (_clock_btn, clock) = Clock::manual().await;
        let analytics_sender = AnalyticsSender::new(Some(tx), clock);
        let loop_handler = analytics_sender.start_loop();
        analytics_sender.send(AnalyticsEvent::Create).await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        analytics_sender
            .send(AnalyticsEvent::EndCommand { return_code: 2i32 })
            .await;
        loop_handler.terminate_analytics().await;
        let payload = rx.recv().await.unwrap();
        assert_eq!(payload.events.len(), 1);
        assert!(matches!(
            &payload.events[0].r#type,
            &AnalyticsEvent::EndCommand { .. }
        ));
    }
}
