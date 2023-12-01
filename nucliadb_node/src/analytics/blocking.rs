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

use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::Mutex;

use crate::analytics::payload::{AnalyticsEvent, AnalyticsPayload};
use crate::analytics::sender::is_analytics_enabled;
use crate::analytics::sink::BlockingHttpClient;

struct SyncAnalyticsLoop {
    client: BlockingHttpClient,
    rtxn: Receiver<AnalyticsEvent>,
}

impl SyncAnalyticsLoop {
    pub fn new(rtxn: Receiver<AnalyticsEvent>) -> Option<SyncAnalyticsLoop> {
        let Some(client) = BlockingHttpClient::try_new() else {
            return None;
        };
        Some(SyncAnalyticsLoop { rtxn, client })
    }
    pub fn run(self) {
        loop {
            match self.rtxn.recv() {
                Err(err) => tracing::info!("channel error {}", err),
                Ok(event) => self
                    .client
                    .blocking_send(AnalyticsPayload::from_single_event(event)),
            }
        }
    }
}

lazy_static::lazy_static! {
    static ref SYNC_ANALYTICS: Option<Mutex<Sender<AnalyticsEvent>>> = {
        if is_analytics_enabled() {
            let (stxn, rtxn) = channel();
            let Some(analytics_loop) = SyncAnalyticsLoop::new(rtxn) else { return None };
            std::thread::spawn(move || analytics_loop.run());
            Some(Mutex::new(stxn))
        } else {
            None
        }
    };
}

pub fn send_analytics_event(event: AnalyticsEvent) {
    if let Some(sender) = SYNC_ANALYTICS.as_ref() {
        // We swallow the error in case of failure
        let sender = sender.lock().unwrap_or_else(|er| er.into_inner());
        if let Err(err) = sender.send(event) {
            tracing::error!("Error sending analytics event: {err:?}");
        }
    }
}
