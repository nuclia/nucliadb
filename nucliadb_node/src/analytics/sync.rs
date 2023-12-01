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

use once_cell::sync::OnceCell;

use crate::analytics::payload::AnalyticsEvent;
pub use crate::analytics::sender::is_analytics_enabled;
use crate::analytics::sender::{AnalyticsLoopHandle, AnalyticsSender};

pub fn start_analytics_loop() -> AnalyticsLoopHandle {
    get_analytics_sender_singleton().start_loop()
}

fn get_analytics_sender_singleton() -> &'static AnalyticsSender {
    static INSTANCE: OnceCell<AnalyticsSender> = OnceCell::new();
    INSTANCE.get_or_init(AnalyticsSender::default)
}

/// Sends a analytics event to Nuclia's server via HTTP.
///
/// Analytics guarantees to send at most 1 request per minute.
/// Each requests can ship at most 10 messages.
///
/// If this methods is called too often, some events will be dropped.
///
/// If the http requests fail, the error will be silent.
///
/// We voluntarily use an enum here to make it easier for reader
/// to audit the type of information that is send home.
pub async fn send_analytics_event(event: AnalyticsEvent) {
    get_analytics_sender_singleton().send(event).await
}
